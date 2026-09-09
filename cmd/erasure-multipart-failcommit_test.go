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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stanford-rc/minio/internal/config/storageclass"
)

// pinParity forces the erasure geometry a test needs, and verifies it took.
//
// ELM 2026-09-09. Setting xl.defaultParityCount is NOT sufficient and fails
// silently. NewMultipartUpload reads globalStorageClass.GetParityForSC first
// (cmd/erasure-multipart.go:413) and only falls back to er.defaultParityCount when
// that returns -1, which it does only while the storage-class config is
// uninitialized. Any earlier test that initializes the config subsystem leaves it
// set, and DefaultParityBlocks(4) is 2, so in a full-package run the field
// assignment is ignored and the object is written at EC:2 instead of EC:1.
//
// That is not hypothetical. TestFailCommitWhenPartBecomesUnreadable passed alone
// and failed in a full run for exactly this reason, and at EC:2 the fault it
// constructs cannot produce an unreadable part at all, which is the arithmetic
// TestUnreadableAfterArithmetic asserts. The assertion was sound; the precondition
// was not being established.
//
// The restore is exact in both directions. Update() unconditionally sets the
// unexported initialized flag, so it cannot express "was never initialized"; for
// that case the zero struct is assigned directly, which resets the flag because it
// replaces the whole value rather than calling a method on it.
func pinParity(t *testing.T, parity int) {
	t.Helper()

	priorCfg := globalStorageClass
	priorParity := globalStorageClass.GetParityForSC("")

	globalStorageClass.Update(storageclass.Config{
		Standard: storageclass.StorageClass{Parity: parity},
	})
	if got := globalStorageClass.GetParityForSC(""); got != parity {
		t.Fatalf("could not pin parity: wanted %d, GetParityForSC reports %d", parity, got)
	}

	t.Cleanup(func() {
		if priorParity < 0 {
			globalStorageClass = storageclass.Config{}
			return
		}
		globalStorageClass.Update(priorCfg)
	})
}

// Fail-the-commit: the third response, for the shape that produced Elm's one
// confirmed permanent loss.
//
// A part is admitted at exactly readQuorum, then renameData commits on writeQuorum
// drives while EXCLUDING one of the drives that part depended on. The part falls
// below dataBlocks, so no reconstruction is possible, and both other responses are
// wrong: refusing at the part boundary never sees the commit, and accepting for
// repair queues a repair that cannot succeed.
//
// The check intersects what readParts recorded about placement with what renameData
// reported as committed, and fails the commit if any part falls below dataBlocks.

// bit is a readable way to write a drive-index bitmask in a test.
func bit(idxs ...int) uint64 {
	var m uint64
	for _, i := range idxs {
		m |= 1 << uint(i)
	}
	return m
}

// The arithmetic, in isolation. No object layer, no MRF, no timing, so nothing here
// can be flaky or pass for an incidental reason.
func TestUnreadableAfterArithmetic(t *testing.T) {
	for _, tc := range []struct {
		name       string
		held       uint64
		committed  uint64
		dataBlocks int
		want       bool
	}{
		{
			name: "whole and fully committed, nothing wrong",
			held: bit(0, 1, 2, 3), committed: bit(0, 1, 2, 3), dataBlocks: 3, want: false,
		},
		{
			name: "part short one drive but all four committed, readable at the floor",
			held: bit(1, 2, 3), committed: bit(0, 1, 2, 3), dataBlocks: 3, want: false,
		},
		{
			name: "THE LOSS: part short one drive, commit excludes a drive that HELD it",
			held: bit(1, 2, 3), committed: bit(0, 1, 2), dataBlocks: 3, want: true,
		},
		{
			name: "commit excludes the drive that did NOT hold the part, still readable",
			held: bit(1, 2, 3), committed: bit(1, 2, 3), dataBlocks: 3, want: false,
		},
		{
			// The same placement and the same exclusion as the loss case, at EC:2.
			// Two surviving shards equals dataBlocks, so it stays readable. This is
			// the arithmetic behind the claim that EC:2 would have prevented the
			// confirmed loss.
			name: "same shape at EC:2 is NOT a loss",
			held: bit(1, 2, 3), committed: bit(0, 1, 2), dataBlocks: 2, want: false,
		},
		{
			name: "two drives lost from the commit set is unreadable at EC:2 too",
			held: bit(1, 2, 3), committed: bit(0, 1), dataBlocks: 2, want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := partPlacement{held: []uint64{tc.held}, usable: bit(0, 1, 2, 3), valid: true}
			got := len(p.unreadableAfter(tc.committed, tc.dataBlocks)) > 0
			if got != tc.want {
				t.Fatalf("held=%04b committed=%04b dataBlocks=%d: got unreadable=%v, want %v",
					tc.held, tc.committed, tc.dataBlocks, got, tc.want)
			}
		})
	}
}

// A part no drive served is already reported through partInfosInQuorum, so reporting
// it again here would double-count. And an over-wide drive set must be skipped rather
// than silently mis-evaluated.
func TestUnreadableAfterSkipsWhatItShould(t *testing.T) {
	p := partPlacement{held: []uint64{0}, usable: bit(0, 1, 2, 3), valid: true}
	if got := p.unreadableAfter(bit(0, 1, 2, 3), 3); len(got) != 0 {
		t.Fatalf("a part held nowhere must not be reported here, got %v", got)
	}

	invalid := partPlacement{held: []uint64{bit(1)}, usable: bit(0, 1, 2, 3), valid: false}
	if got := invalid.unreadableAfter(bit(0), 3); len(got) != 0 {
		t.Fatalf("an unusable placement must be skipped, not evaluated; got %v", got)
	}
}

// failRenameDataDisk fails RenameData on demand while staying online, which is how a
// commit excludes a drive without the drive being considered down.
type failRenameDataDisk struct {
	StorageAPI
	failing bool
}

func (d *failRenameDataDisk) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo,
	dstVolume, dstPath string, opts RenameOptions,
) (RenameDataResp, error) {
	if d.failing {
		return RenameDataResp{}, errFaultyDisk
	}
	return d.StorageAPI.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath, opts)
}

func (d *failRenameDataDisk) IsOnline() bool { return true }

// removeStagedPartFrom deletes a staged part and its sidecar from ONE named backing
// dir, so the test controls which drive is short rather than taking whichever the
// filesystem walk reached first.
func removeStagedPartFrom(t *testing.T, dir, part string) {
	t.Helper()
	var metaPath string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || metaPath != "" {
			return nil
		}
		if !d.IsDir() && d.Name() == part+".meta" &&
			strings.Contains(filepath.ToSlash(p), minioMetaMultipartBucket) {
			metaPath = p
		}
		return nil
	})
	if metaPath == "" {
		t.Fatalf("no staged %s.meta under %s", part, dir)
	}
	if err := os.Remove(metaPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(filepath.Dir(metaPath), part)); err != nil {
		t.Fatal(err)
	}
}

// stagingSurvives reports whether any staged part.N remains, which is the evidence
// that the recovery window was held open rather than closed by the success-path
// cleanup.
func stagingSurvives(fsDirs []string, part string) int {
	n := 0
	for _, dir := range fsDirs {
		found := false
		_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if !d.IsDir() && d.Name() == part &&
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

// End to end at EC:1, which is Elm's geometry and the only one where a single
// commit-set exclusion can push a 3-of-4 part below the readable threshold.
func TestFailCommitWhenPartBecomesUnreadable(t *testing.T) {
	// Not the subject. The state is constructed directly, so the part boundary must
	// not get a say.
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, fsDirs, err := prepareErasure(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	if err := obj.MakeBucket(ctx, "bucket", MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	// EC:1 gives dataBlocks 3 and writeQuorum 3, so renameData tolerates exactly one
	// failure and a part on three drives is exactly at the readable floor. At the
	// default EC:2 this fault cannot produce an unreadable part at all, which is the
	// point asserted in TestUnreadableAfterArithmetic.
	xl.defaultParityCount = 1
	pinParity(t, 1)

	// Wrap drive 1 to fail RenameData. Drive 0 is the one whose staged shard is
	// removed below, so the excluded drive and the short drive differ, which is what
	// makes the intersection fall to two.
	erasureDisks := xl.getDisks()
	victim := &failRenameDataDisk{StorageAPI: erasureDisks[1]}
	z.serverPools[0].erasureDisksMu.Lock()
	erasureDisks[1] = victim
	xl.getDisks = func() []StorageAPI { return erasureDisks }
	z.serverPools[0].erasureDisksMu.Unlock()

	res, err := obj.NewMultipartUpload(ctx, "bucket", "collapse", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}

	data := bytes.Repeat([]byte("a"), 1<<20)
	pi, err := obj.PutObjectPart(ctx, "bucket", "collapse", res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("clean PutObjectPart: %v", err)
	}

	// Stage 1 of the traced loss: the part is admitted at three of four.
	removeStagedPartFrom(t, fsDirs[0], "part.1")

	// Stage 2: the commit excludes a different drive, one that DID hold the part.
	victim.failing = true

	_, err = obj.CompleteMultipartUpload(ctx, "bucket", "collapse", res.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: pi.ETag}}, ObjectOptions{})

	if err == nil {
		t.Fatal("the commit must FAIL: the part is held on two drives and needs three, " +
			"so returning success would promise data that cannot be read back")
	}
	t.Logf("commit correctly refused: %v", err)

	// The reason this response is better than either alternative, and the property
	// worth asserting rather than just the retention: RECOVERY IS STILL POSSIBLE.
	//
	// The part was held on drives 1, 2 and 3. renameData committed on 0, 2 and 3, so
	// the committed copies are on 2 and 3, which is two and below the three needed.
	// Drive 1's copy did not commit, and it is still in staging because the
	// success-path cleanup is conditional on err == nil. Committed plus staged is
	// therefore three, which is exactly enough to reconstruct.
	//
	// Had the commit returned 200, that deferred cleanup would have run and deleted
	// the staged copy, leaving two and nothing to rebuild from. That is the step the
	// traced loss report identifies as destroying "the only copies that could have
	// repaired the shortfall", 4m39s after the commit.
	staged := stagingSurvives(fsDirs, "part.1")
	committed := 0
	for _, dir := range fsDirs {
		if len(findCommittedPart(dir, "collapse", "part.1")) > 0 {
			committed++
		}
	}
	t.Logf("after the refused commit: part.1 committed on %d drive(s), staged on %d",
		committed, staged)

	if staged == 0 {
		t.Fatal("failing the commit must leave the staging shards in place, " +
			"since they are the only copies that could still repair the object")
	}
	if committed+staged < 3 {
		t.Fatalf("recovery must remain possible: committed %d plus staged %d is below "+
			"the 3 shards EC:1 needs, so refusing the commit bought nothing",
			committed, staged)
	}
}

// findCommittedPart returns the path of a committed part under the bucket prefix, as
// opposed to the staging area.
func findCommittedPart(dir, object, part string) string {
	var found string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || found != "" {
			return nil
		}
		s := filepath.ToSlash(p)
		if !d.IsDir() && d.Name() == part &&
			strings.Contains(s, "/bucket/"+object+"/") &&
			!strings.Contains(s, minioMetaBucket) {
			found = p
		}
		return nil
	})
	return found
}

// The control. The identical injection with the part intact on all four drives must
// commit normally, or the check above would be indistinguishable from one that
// refuses whenever renameData loses a drive.
func TestFailCommitLeavesHealthyCommitAlone(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, fsDirs, err := prepareErasure(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	if err := obj.MakeBucket(ctx, "bucket", MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]
	xl.defaultParityCount = 1
	pinParity(t, 1)

	erasureDisks := xl.getDisks()
	victim := &failRenameDataDisk{StorageAPI: erasureDisks[1]}
	z.serverPools[0].erasureDisksMu.Lock()
	erasureDisks[1] = victim
	xl.getDisks = func() []StorageAPI { return erasureDisks }
	z.serverPools[0].erasureDisksMu.Unlock()

	res, err := obj.NewMultipartUpload(ctx, "bucket", "healthy", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}
	data := bytes.Repeat([]byte("a"), 1<<20)
	pi, err := obj.PutObjectPart(ctx, "bucket", "healthy", res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("clean PutObjectPart: %v", err)
	}

	// No part removed. The commit still loses a drive, so the part goes from four
	// holders to three, which is the readable floor and must be allowed through.
	victim.failing = true

	if _, err := obj.CompleteMultipartUpload(ctx, "bucket", "healthy", res.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: pi.ETag}}, ObjectOptions{}); err != nil {
		t.Fatalf("a part that is still at the readable floor must commit: %v", err)
	}
}
