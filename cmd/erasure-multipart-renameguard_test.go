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
	"sort"
	"strings"
	"testing"
)

// The rename-site guard: enforceWriteSet called a second time, after renamePart.
//
// Unlike erasure-multipart-renamepart_test.go, this file asserts PATCHED behavior
// and is expected to fail on a tree without the guard. That file remains the
// unpatched-compilable control.

// stagedPartDirs reports which backing dirs hold a staged part.N, sorted, so a test
// can assert WHICH drives are short rather than only how many.
func stagedPartDirs(t *testing.T, fsDirs []string, part string) []string {
	t.Helper()

	var out []string
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
			out = append(out, dir)
		}
	}
	sort.Strings(out)
	return out
}

// putSizedPart uploads one part of the given size and returns the error, if any.
func putSizedPart(ctx context.Context, t *testing.T, obj ObjectLayer, object, uploadID string, partID, size int) error {
	t.Helper()
	data := bytes.Repeat([]byte("a"), size)
	_, err := obj.PutObjectPart(ctx, "bucket", object, uploadID, partID,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	return err
}

// setupMixedFaultTest wraps drive 0 to fail CreateFile and drive 1 to fail
// RenamePart, so one part can be made to take a shortfall at BOTH narrowing points
// and on different drives.
func setupMixedFaultTest(ctx context.Context, t *testing.T) (ObjectLayer, *failCreateFileDisk, *failRenamePartDisk, []string) {
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
	cf := &failCreateFileDisk{StorageAPI: erasureDisks[0]}
	rp := &failRenamePartDisk{StorageAPI: erasureDisks[1]}
	z.serverPools[0].erasureDisksMu.Lock()
	erasureDisks[0] = cf
	erasureDisks[1] = rp
	xl.getDisks = func() []StorageAPI { return erasureDisks }
	z.serverPools[0].erasureDisksMu.Unlock()

	return obj, cf, rp, fsDirs
}

// T1.3. The rename site applies the same single policy as the encode site: a
// healable shortfall is accepted and left to the commit boundary.
//
// The reason this test still exists after the policy collapsed to one branch is
// that the rename site is a SEPARATE narrowing point. TestRenamePartShortfallIsSilent
// proves the encode-site check cannot see a rename failure at all, so "accepted"
// here has to be shown to be a decision rather than an absence of code.
func TestRenameGuardShortfallIsAcceptedWhenHealable(t *testing.T) {
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

			obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
			defer obj.Shutdown(context.Background())
			defer removeRoots(fsDirs)

			res, err := obj.NewMultipartUpload(ctx, "bucket", "matrix", ObjectOptions{})
			if err != nil {
				t.Fatalf("NewMultipartUpload: %v", err)
			}

			wrapped[0].failing.Store(true)
			err = putSizedPart(ctx, t, obj, "matrix", res.UploadID, 1, 1<<20)

			if wrapped[0].calls.Load() == 0 {
				t.Fatal("RenamePart was never called on the injected drive, so this case proved nothing")
			}
			if err != nil {
				t.Fatalf("a healable rename shortfall must be accepted, not refused: %v", err)
			}
		})
	}
}

// A clean rename must never be refused, in any mode. Without this the matrix above
// would pass for a check that always refused.
func TestRenameGuardCleanUploadNeverRefused(t *testing.T) {
	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINIO_MULTIPART_WRITESET", mode)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
			defer obj.Shutdown(context.Background())
			defer removeRoots(fsDirs)

			res, err := obj.NewMultipartUpload(ctx, "bucket", "clean", ObjectOptions{})
			if err != nil {
				t.Fatalf("NewMultipartUpload: %v", err)
			}

			wrapped[0].failing.Store(false)
			if err := putSizedPart(ctx, t, obj, "clean", res.UploadID, 1, 1<<20); err != nil {
				t.Fatalf("clean upload must not be affected in %q mode: %v", mode, err)
			}
			if wrapped[0].calls.Load() == 0 {
				t.Fatal("the wrapped drive was never asked to RenamePart")
			}
			if got := stagedPartDirs(t, fsDirs, "part.1"); len(got) != 4 {
				t.Fatalf("clean upload must stage part.1 on all 4 drives, got %d", len(got))
			}
		})
	}
}

// T1.4. A re-sent part lands on ALL FOUR drives once the fault clears, which is
// the on-disk geometry check behind the whole write-set idea: a part that landed on
// three drives and one that landed on four are indistinguishable to the client and
// differ by the entire parity margin.
//
// Under the old reject policy this test drove the re-send by having the first
// attempt refused. The policy now accepts a healable shortfall, so the re-send is
// the client overwriting the same part number, which is what S3 permits and what a
// retrying client actually does.
func TestRenameGuardResendLandsEverywhere(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "on")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	res, err := obj.NewMultipartUpload(ctx, "bucket", "retry", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}

	wrapped[0].failing.Store(true)
	if err := putSizedPart(ctx, t, obj, "retry", res.UploadID, 1, 1<<20); err != nil {
		t.Fatalf("the healable shortfall should have been accepted: %v", err)
	}
	if got := stagedPartDirs(t, fsDirs, "part.1"); len(got) != 3 {
		t.Fatalf("precondition: the shortfall should have staged part.1 on 3 of 4 drives, got %d", len(got))
	}

	// The transient fault clears, exactly as a reconnected peer would.
	wrapped[0].failing.Store(false)
	if err := putSizedPart(ctx, t, obj, "retry", res.UploadID, 1, 1<<20); err != nil {
		t.Fatalf("re-sent part should have been accepted: %v", err)
	}

	if got := stagedPartDirs(t, fsDirs, "part.1"); len(got) != 4 {
		t.Fatalf("a re-send must restore FULL redundancy, not the erasure minimum; "+
			"part.1 staged on %d of 4 drives", len(got))
	}
}

// T1.6. Both narrowings against the SAME part, on different drives. Under EC:2 on
// four drives writeQuorum is 3, so losing one drive at encode and another at rename
// leaves 2 renames and renamePart's own quorum check refuses first.
//
// Asserts a FLOOR rather than which layer refused: with enough drives failing the
// error may come from erasure.Encode, from renamePart's reduceWriteQuorumErrs, or
// from the guard. All three are correct answers to the same question. What must
// never happen is success with the part on fewer drives than data blocks.
func TestRenameGuardBothFaultsSamePartNeverSucceeds(t *testing.T) {
	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINIO_MULTIPART_WRITESET", mode)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			obj, cf, rp, fsDirs := setupMixedFaultTest(ctx, t)
			defer obj.Shutdown(context.Background())
			defer removeRoots(fsDirs)

			res, err := obj.NewMultipartUpload(ctx, "bucket", "both", ObjectOptions{})
			if err != nil {
				t.Fatalf("NewMultipartUpload: %v", err)
			}

			cf.failing.Store(true)
			rp.failing.Store(true)
			err = putSizedPart(ctx, t, obj, "both", res.UploadID, 1, 1<<20)

			if cf.calls.Load() == 0 {
				t.Fatal("the CreateFile-failing drive was never written to")
			}
			if err == nil {
				staged := stagedPartDirs(t, fsDirs, "part.1")
				t.Fatalf("a part that lost a drive at encode AND another at rename must not be "+
					"accepted in %q mode; it staged on %d of 4 drives", mode, len(staged))
			}
			t.Logf("%q mode: refused as required (%v)", mode, err)
		})
	}
}

// T1.7. The same two faults spread across DIFFERENT parts of one upload. Each part
// is evaluated independently, so each is short on a different single drive and both
// remain reconstructable. This is the case that must NOT be escalated: per-part
// repair is exactly what read-elm-object --part does, and treating it as an
// object-level emergency would refuse uploads that are entirely recoverable.
//
// Runs with enforcement off because the point is the resulting on-disk geometry
// rather than the policy, and off is the only setting that emits no log line for a
// shortfall it accepts.
func TestRenameGuardFaultsOnDifferentPartsStayIndependent(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, cf, rp, fsDirs := setupMixedFaultTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	res, err := obj.NewMultipartUpload(ctx, "bucket", "split", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}

	// Part 1 loses a shard at the encode site on drive 0. Sized above
	// globalMinPartSize because it is not the last part.
	cf.failing.Store(true)
	if err := putSizedPart(ctx, t, obj, "split", res.UploadID, 1, 6<<20); err != nil {
		t.Fatalf("part 1 should have been accepted in off mode: %v", err)
	}
	cf.failing.Store(false)

	// Part 2 loses a shard at the rename site on drive 1.
	rp.failing.Store(true)
	if err := putSizedPart(ctx, t, obj, "split", res.UploadID, 2, 1<<20); err != nil {
		t.Fatalf("part 2 should have been accepted in off mode: %v", err)
	}
	rp.failing.Store(false)

	p1 := stagedPartDirs(t, fsDirs, "part.1")
	p2 := stagedPartDirs(t, fsDirs, "part.2")
	t.Logf("part.1 staged on %d drives, part.2 on %d drives", len(p1), len(p2))

	if len(p1) != 3 || len(p2) != 3 {
		t.Fatalf("each part should be short on exactly one drive; part.1 on %d, part.2 on %d",
			len(p1), len(p2))
	}

	// The drives differ, which is what makes this per-part rather than a drive-wide
	// failure. If they matched, a single drive dropped both and the object would be
	// one drive short overall instead of each part being independently short.
	if strings.Join(p1, ",") == strings.Join(p2, ",") {
		t.Fatalf("the two parts are short on the SAME drive set (%v), so this case is not "+
			"testing independent per-part evaluation", p1)
	}

	// Both parts still have enough shards to reconstruct, so this is a heal job and
	// not a loss. Under EC:2 on four drives data blocks is 2.
	for _, p := range []struct {
		name string
		dirs []string
	}{{"part.1", p1}, {"part.2", p2}} {
		if len(p.dirs) < 2 {
			t.Fatalf("%s is below the data-block count and would be unrecoverable", p.name)
		}
	}
}
