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
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// PutObjectPart narrows the usable drive set TWICE, and only the first narrowing
// is guarded.
//
// erasure.Encode is the known one: multiWriter nils a failed writer and swallows
// the error, so a part can be admitted on 3 of 4 drives behind a 200. That is what
// erasure-multipart-writeset_test.go covers.
//
// renamePart is the second, and it happens AFTER that check. It moves the shard
// from .minio.sys/tmp to .minio.sys/multipart/<uploadIDPath>/<dataDir>/part.N and
// ends with:
//
//	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
//	return evalDisks(disks, errs), err
//
// With 3 of 4 renames succeeding, reduceWriteQuorumErrs returns nil, evalDisks
// nils the fourth drive, and the narrowed slice is discarded when PutObjectPart
// returns. reduceQuorumErrs does no logging, and the injected errFaultyDisk is a
// member of objectOpIgnoredErrs, so the drive's error is discarded rather than
// merely outvoted. Nothing is logged and nothing is queued.
//
// The end state is identical to the Encode fault -- part.N missing on one drive,
// no tmp litter because of the deleteAll defer, no log line -- which is why the
// production litter census cannot tell the two apart.
//
// This file deliberately references NO symbol introduced by the write-set patch,
// so it compiles and runs against the unpatched tree. That is what makes T1.1 a
// usable control: it must pass unpatched, and it must still pass with A-prime
// applied, because A-prime's check sits upstream of the site it exercises.
//
// That constraint is also why these tests do not call resetWriteSetBreaker():
// it lives in erasure-multipart-writeset_test.go, which is absent during the
// unpatched run. They need no reset in any case, because the rename site is
// unguarded, so it neither reads nor records breaker events.
type failRenamePartDisk struct {
	StorageAPI
	failing atomic.Bool
	calls   atomic.Int64
}

func (d *failRenamePartDisk) RenamePart(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string, meta []byte, skipParent string) error {
	d.calls.Add(1)
	if d.failing.Load() {
		return errFaultyDisk
	}
	return d.StorageAPI.RenamePart(ctx, srcVolume, srcPath, dstVolume, dstPath, meta, skipParent)
}

// Healthy from the cluster's point of view, which is the whole problem. Both
// traced production losses were drives that stayed online throughout.
func (d *failRenamePartDisk) IsOnline() bool { return true }

// setupRenamePartTest returns an object layer whose first nDown drives fail
// RenamePart while still reporting themselves online.
//
// Deliberately self-contained rather than sharing setupWriteSetTest, because the
// baseline run needs this file to compile with erasure-multipart-writeset_test.go
// absent.
func setupRenamePartTest(ctx context.Context, t *testing.T, nDown int) (ObjectLayer, []*failRenamePartDisk, []string) {
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
	wrapped := make([]*failRenamePartDisk, 0, nDown)
	z.serverPools[0].erasureDisksMu.Lock()
	for i := 0; i < nDown; i++ {
		w := &failRenamePartDisk{StorageAPI: erasureDisks[i]}
		erasureDisks[i] = w
		wrapped = append(wrapped, w)
	}
	xl.getDisks = func() []StorageAPI { return erasureDisks }
	z.serverPools[0].erasureDisksMu.Unlock()

	return obj, wrapped, fsDirs
}

// countCommittedPartDirs reports how many backing dirs hold a file named `name`
// somewhere under minioMetaMultipartBucket, which is where a successfully renamed
// part lands.
//
// Scoping to the multipart prefix rather than counting every match makes the
// assertion independent of whether the tmp cleanup defer has run yet, and walking
// rather than reconstructing the path keeps it independent of getUploadIDDir's
// layout.
func countCommittedPartDirs(t *testing.T, fsDirs []string, name string) int {
	t.Helper()

	n := 0
	for _, dir := range fsDirs {
		found := false
		_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if !d.IsDir() && d.Name() == name &&
				strings.Contains(filepath.ToSlash(p), minioMetaMultipartBucket) {
				found = true
			}
			return nil
		})
		if found {
			n++
		}
	}
	return n
}

// T1.1 / T1.2. Unpatched this documents the defect. With A-prime applied it must
// STILL pass, which is the evidence that the patch's check at the Encode site does
// not cover the rename site.
func TestRenamePartShortfallIsSilent(t *testing.T) {
	// Pinned to off, for two reasons. It keeps this a statement about the UNGUARDED
	// behaviour of renamePart, which is what makes it a usable control on an
	// unpatched tree where the variable is simply ignored. And once the rename-site
	// guard exists the default became reject, which would refuse the part and turn
	// this case into a duplicate of TestRenameGuardShortfallModeMatrix.
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	res, err := obj.NewMultipartUpload(ctx, "bucket", "silent", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}

	// Armed only now, so nothing NewMultipartUpload does can be blamed for the
	// result.
	wrapped[0].failing.Store(true)

	data := bytes.Repeat([]byte("a"), 1<<20)
	_, err = obj.PutObjectPart(ctx, "bucket", "silent", res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})

	if wrapped[0].calls.Load() == 0 {
		t.Fatal("RenamePart was never called on the injected drive, so this case proved nothing")
	}
	if err != nil {
		t.Fatalf("expected the part to be ACCEPTED, which is the defect being documented; got %v", err)
	}

	if got := countCommittedPartDirs(t, fsDirs, "part.1"); got != 3 {
		t.Fatalf("expected part.1 committed on 3 of 4 drives, got %d", got)
	}
}

// The control for the control. A drive that takes every rename must not be
// reported as a shortfall, or the case above would pass for the wrong reason.
func TestRenamePartCleanUploadLandsEverywhere(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	res, err := obj.NewMultipartUpload(ctx, "bucket", "clean", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}

	data := bytes.Repeat([]byte("a"), 1<<20)
	if _, err = obj.PutObjectPart(ctx, "bucket", "clean", res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
		t.Fatalf("clean upload must succeed: %v", err)
	}

	if wrapped[0].calls.Load() == 0 {
		t.Fatal("RenamePart was never called, so the wrapper is not in the path at all")
	}
	if got := countCommittedPartDirs(t, fsDirs, "part.1"); got != 4 {
		t.Fatalf("expected part.1 committed on all 4 drives, got %d", got)
	}
}
