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
	"slices"
	"sync/atomic"
	"testing"
)

// A part can be admitted on fewer drives than were attempted and still return
// success, because erasure.Encode returns as soon as writeQuorum drives accept
// and multiWriter nils the writer that failed while swallowing its error
// (cmd/erasure-encode.go). Nothing downstream compares the set: the only
// self-repair hook in CompleteMultipartUpload asks whether a drive is OFFLINE,
// and a drive that merely lost one write is still online. The result is a shard
// that never existed, behind an HTTP 200.
//
// No test covered that, which is why it survived. These do.
//
// The wrapper below fails only CreateFile, and only on the drives asked for. It
// deliberately reports IsOnline() as true, because that is the production shape:
// both traced losses were drives that stayed healthy throughout. naughtyDisk is
// unsuitable here on two counts -- its call counter is global across all 35 gated
// methods so it cannot target one operation, and its IsOnline() returns false for
// anything but errDiskNotFound, which would send these cases down the
// already-known-bad path instead of the one under test.
type failCreateFileDisk struct {
	StorageAPI
	failing atomic.Bool
	calls   atomic.Int64
}

func (d *failCreateFileDisk) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	d.calls.Add(1)
	if d.failing.Load() {
		return errFaultyDisk
	}
	return d.StorageAPI.CreateFile(ctx, origvolume, volume, path, size, reader)
}

// Healthy from the cluster's point of view, which is the whole problem.
func (d *failCreateFileDisk) IsOnline() bool { return true }

// setupWriteSetTest returns an object layer whose first nDown drives fail
// CreateFile while still reporting themselves online.
func setupWriteSetTest(ctx context.Context, t *testing.T, nDown int) (ObjectLayer, []*failCreateFileDisk, []string) {
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
	wrapped := make([]*failCreateFileDisk, 0, nDown)
	z.serverPools[0].erasureDisksMu.Lock()
	for i := 0; i < nDown; i++ {
		w := &failCreateFileDisk{StorageAPI: erasureDisks[i]}
		erasureDisks[i] = w
		wrapped = append(wrapped, w)
	}
	xl.getDisks = func() []StorageAPI { return erasureDisks }
	z.serverPools[0].erasureDisksMu.Unlock()

	return obj, wrapped, fsDirs
}

func putOnePart(ctx context.Context, t *testing.T, obj ObjectLayer, object string) error {
	t.Helper()
	res, err := obj.NewMultipartUpload(ctx, "bucket", object, ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}
	// Small on purpose: the shortfall check is about which drives took the write,
	// not about how much was written, and globalMinPartSize is only enforced at
	// CompleteMultipartUpload, which these cases never reach.
	data := bytes.Repeat([]byte("a"), 1<<20)
	_, err = obj.PutObjectPart(ctx, "bucket", object, res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	return err
}

// THE POLICY, in one test. A healable shortfall is ACCEPTED so the client is not
// made to re-send terabytes, and the commit boundary queues the repair. Only an
// unrecoverable shortfall is refused, and that case is pinned separately below.
//
// This asserts the OPPOSITE of what the first revision of this patch asserted.
// Until 2026-09-08 a healable shortfall was refused by default, on the theory that
// a re-send restores full redundancy where a heal reconstructs to the erasure
// minimum. That is true and was still the wrong trade: production failures arrive
// in bursts, so a re-send often meets the same fault, and 90.48% of PutObjectPart
// is a client that cancels and deletes the whole in-flight batch on one 503.
func TestMultipartPartWriteSetShortfallIsAcceptedWhenHealable(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
	}{
		{"enforcement on", "on"},
		{"enforcement off", "off"},
		{"unset defaults to on", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MINIO_MULTIPART_WRITESET", tc.env)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			obj, wrapped, fsDirs := setupWriteSetTest(ctx, t, 1)
			defer obj.Shutdown(context.Background())
			defer removeRoots(fsDirs)

			wrapped[0].failing.Store(true)
			if err := putOnePart(ctx, t, obj, "shortfall"); err != nil {
				t.Fatalf("a healable shortfall must be accepted, not refused: %v", err)
			}

			if wrapped[0].calls.Load() == 0 {
				t.Fatal("the injected drive was never asked to CreateFile, so this case proved nothing")
			}
		})
	}
}

// A drive that takes every write must never be refused. This is the control:
// without it, a check that simply always succeeded would pass the case above.
func TestMultipartPartWriteSetCleanUpload(t *testing.T) {
	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINIO_MULTIPART_WRITESET", mode)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			obj, wrapped, fsDirs := setupWriteSetTest(ctx, t, 1)
			defer obj.Shutdown(context.Background())
			defer removeRoots(fsDirs)

			// Wrapped but not failing: the write path is identical to an
			// unwrapped drive.
			wrapped[0].failing.Store(false)
			if err := putOnePart(ctx, t, obj, "clean"); err != nil {
				t.Fatalf("clean upload must not be affected in %q mode: %v", mode, err)
			}
			if wrapped[0].calls.Load() == 0 {
				t.Fatal("the wrapped drive was never written to")
			}
		})
	}
}

// A shortfall must not poison the upload: a drive that recovers is written to
// again on the next part, and the next part is clean. Under the old reject policy
// this test proved a refused part could be re-sent; it now proves the weaker but
// still necessary property that accepting a shortfall leaves the path usable.
func TestMultipartPartWriteSetRecoveryIsClean(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "on")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, wrapped, fsDirs := setupWriteSetTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	wrapped[0].failing.Store(true)
	if err := putOnePart(ctx, t, obj, "recover"); err != nil {
		t.Fatalf("healable shortfall should have been accepted: %v", err)
	}

	// The transient fault clears, exactly as a reconnected peer or a recovered
	// drive would.
	wrapped[0].failing.Store(false)
	before := wrapped[0].calls.Load()
	if err := putOnePart(ctx, t, obj, "recover"); err != nil {
		t.Fatalf("part after recovery should have been accepted: %v", err)
	}
	if wrapped[0].calls.Load() <= before {
		t.Fatal("the recovered drive was not written to on the second part")
	}
}

// Unrecoverable is refused even with enforcement switched off, because returning
// success there would promise durability the cluster cannot deliver: fewer
// surviving shards than data blocks cannot be reconstructed from parity at all.
// This is the ONLY case the part boundary refuses.
//
// Note this asserts a floor rather than the exact boundary. With enough drives
// failing, erasure.Encode fails write quorum on its own and returns first, so the
// error may come from either check. Both are correct answers to the same
// question; what must never happen is success.
func TestMultipartPartWriteSetUnrecoverableAlwaysRefused(t *testing.T) {
	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINIO_MULTIPART_WRITESET", mode)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Three of four drives refusing the shard leaves one, which is below
			// the data-block count for any parity setting on a four-drive set.
			obj, wrapped, fsDirs := setupWriteSetTest(ctx, t, 3)
			defer obj.Shutdown(context.Background())
			defer removeRoots(fsDirs)

			for _, w := range wrapped {
				w.failing.Store(true)
			}
			if err := putOnePart(ctx, t, obj, "unrecoverable"); err == nil {
				t.Fatalf("an unrecoverable part was accepted in %q mode", mode)
			}
		})
	}
}

// The sliding-window breaker that used to live here was REMOVED 2026-09-08 along
// with reject mode, and its three tests with it. It made the server's answer to
// identical input depend on unrelated concurrent traffic. If it ever comes back,
// the properties worth re-pinning were: it opens on the Nth shortfall inside the
// window, it recovers once the window passes, and end to end the first shortfall
// is refused while a later one in the same window is accepted.

// healableDivergence is the cross-check between MinIO's count-based verdict and
// ours. It must never change behavior, so the only thing to test is that it
// classifies correctly, including the case that motivates it: MinIO says a part is
// fine while the committed drives cannot reconstruct it.
func TestHealableDivergence(t *testing.T) {
	const dataBlocks = 3

	ok := ObjectPartInfo{Number: 1}
	bad := ObjectPartInfo{Number: 1, Error: "InvalidPart"}

	for _, tc := range []struct {
		name      string
		placement partPlacement
		infos     []ObjectPartInfo
		committed uint64
		wantOpt   []int
		wantPess  []int
	}{
		{
			name:      "agreement, both say healable",
			placement: partPlacement{held: []uint64{0b1111}, valid: true},
			infos:     []ObjectPartInfo{ok},
			committed: 0b1111,
		},
		{
			name:      "agreement, both say unhealable",
			placement: partPlacement{held: []uint64{0b0011}, valid: true},
			infos:     []ObjectPartInfo{bad},
			committed: 0b1111,
			// 2 held of 3 needed, and readParts also errored: no divergence.
		},
		{
			name: "OPTIMISTIC: readParts accepts, committed set cannot reconstruct",
			// The part is on 4 drives but only 2 of them committed, so the
			// intersection is below dataBlocks while the ETag count was fine.
			placement: partPlacement{held: []uint64{0b1111}, valid: true},
			infos:     []ObjectPartInfo{ok},
			committed: 0b0011,
			wantOpt:   []int{0},
		},
		{
			name:      "PESSIMISTIC: readParts errors, committed set is fine",
			placement: partPlacement{held: []uint64{0b1111}, valid: true},
			infos:     []ObjectPartInfo{bad},
			committed: 0b1111,
			wantPess:  []int{0},
		},
		{
			name:      "an untracked placement reports nothing",
			placement: partPlacement{held: []uint64{0b0001}, valid: false},
			infos:     []ObjectPartInfo{ok},
			committed: 0b1111,
		},
		{
			name:      "a part no drive served is skipped, not reported",
			placement: partPlacement{held: []uint64{0}, valid: true},
			infos:     []ObjectPartInfo{bad},
			committed: 0b1111,
		},
		{
			name:      "a short partInfos slice is skipped rather than panicking",
			placement: partPlacement{held: []uint64{0b1111, 0b0001}, valid: true},
			infos:     []ObjectPartInfo{ok},
			committed: 0b0011,
			wantOpt:   []int{0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt, pess := healableDivergence(tc.placement, tc.infos, tc.committed, dataBlocks)
			if !slices.Equal(opt, tc.wantOpt) {
				t.Errorf("optimistic: got %v, want %v", opt, tc.wantOpt)
			}
			if !slices.Equal(pess, tc.wantPess) {
				t.Errorf("pessimistic: got %v, want %v", pess, tc.wantPess)
			}
		})
	}
}
