// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
)

// The write-set patch splits a shortfall by the health of the drive that dropped
// the write:
//
//	if attemptedDisks[i].IsOnline() { lostHealthy++ } else { lostUnhealthy++ }
//
// and only refuses when lostHealthy > 0. The reasoning is that a drive which
// failed while still reporting itself healthy is a transient fault the client can
// retry past, whereas refusing writes on behalf of a drive already known bad would
// take the tier down rather than protect it.
//
// The lostUnhealthy branch has never executed. Every earlier attempt to make a
// drive unhealthy also broke the unpatched baseline, so the branch is a safeguard
// on paper only. failOfflineDisk exists to drive it directly: it fails CreateFile
// AND reports itself offline, which no real injection managed to do in isolation.
type failOfflineDisk struct {
	StorageAPI
	failing atomic.Bool
	calls   atomic.Int64
}

func (d *failOfflineDisk) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	d.calls.Add(1)
	if d.failing.Load() {
		return errFaultyDisk
	}
	return d.StorageAPI.CreateFile(ctx, origvolume, volume, path, size, reader)
}

// The distinguishing property. failCreateFileDisk reports true.
func (d *failOfflineDisk) IsOnline() bool { return false }

func setupOfflineWriteSetTest(ctx context.Context, t *testing.T) (ObjectLayer, *failOfflineDisk, []string) {
	t.Helper()

	obj, fsDirs, err := prepareErasure(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "bucket", MakeBucketOptions{}); err != nil {
		removeRoots(fsDirs)
		t.Fatal(err)
	}

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	erasureDisks := xl.getDisks()
	w := &failOfflineDisk{StorageAPI: erasureDisks[0]}
	z.serverPools[0].erasureDisksMu.Lock()
	erasureDisks[0] = w
	xl.getDisks = func() []StorageAPI { return erasureDisks }
	z.serverPools[0].erasureDisksMu.Unlock()

	return obj, w, fsDirs
}

func putPart(ctx context.Context, t *testing.T, obj ObjectLayer, object, uploadID string, partID int) error {
	t.Helper()
	data := bytes.Repeat([]byte("a"), 1<<20)
	_, err := obj.PutObjectPart(ctx, "bucket", object, uploadID, partID,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	return err
}

// T1.5. Both shapes of healable shortfall are accepted: one where the drive that
// dropped the write still reports itself online, and one where it is already known
// bad. The code still separates them into lostHealthy and lostUnhealthy, but as of
// 2026-09-08 that distinction only labels the log line; it gates no behavior.
//
// COVERAGE NOTE. Under the old reject policy this test asserted a PAIR, with the
// offline case accepted and the healthy case refused, and that asymmetry was what
// made it impossible to pass on an unpatched tree. That property is gone with
// reject: an unpatched tree also accepts both of these. The remaining assertion is
// that neither shape is refused, which is a regression guard against
// over-refusing, not a proof the guard exists. What proves the guard exists is now
// TestMultipartPartWriteSetUnrecoverableAlwaysRefused and the commit-boundary
// tests.
func TestWriteSetHealableShortfallsAreAccepted(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "on")

	t.Run("offline drive is accepted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		obj, w, fsDirs := setupOfflineWriteSetTest(ctx, t)
		defer obj.Shutdown(context.Background())
		defer removeRoots(fsDirs)

		res, err := obj.NewMultipartUpload(ctx, "bucket", "offline", ObjectOptions{})
		if err != nil {
			t.Fatalf("NewMultipartUpload: %v", err)
		}

		w.failing.Store(true)
		err = putPart(ctx, t, obj, "offline", res.UploadID, 1)

		if w.calls.Load() == 0 {
			t.Fatal("the injected drive was never asked to CreateFile, so this case proved nothing")
		}
		if err != nil {
			t.Fatalf("a drive already known bad must not cause a refusal, or a degraded tier "+
				"stops accepting uploads altogether; got %v", err)
		}
	})

	t.Run("healthy drive is also accepted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		obj, wrapped, fsDirs := setupWriteSetTest(ctx, t, 1)
		defer obj.Shutdown(context.Background())
		defer removeRoots(fsDirs)

		wrapped[0].failing.Store(true)
		err := putOnePart(ctx, t, obj, "healthy")

		if wrapped[0].calls.Load() == 0 {
			t.Fatal("the injected drive was never asked to CreateFile")
		}
		if err != nil {
			t.Fatalf("a healable shortfall on a healthy drive must be accepted so the client "+
				"is not made to re-send; got %v", err)
		}
	})
}

// T1.8 tested globalWriteSetBreaker under concurrent PutObjectPart calls, because
// it was shared mutable state reached from every one of them and partIDLock only
// serializes calls for the same part ID. REMOVED 2026-09-08 with the breaker: the
// enforcement path now holds no mutable package state at all, so there is nothing
// left for concurrent parts to race on. If any is reintroduced, restore a -race
// test that drives distinct part IDs concurrently through a shortfall.
