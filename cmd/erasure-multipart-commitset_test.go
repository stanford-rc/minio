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

	"github.com/minio/madmin-go/v3"
)

// Fix A: the commit-boundary half of the write-set work.
//
// TestRenamePartShortfallIsSilent proves the part-boundary check cannot see a
// renamePart shortfall, because it runs at line ~690 and renamePart is at ~762.
// These tests prove the commit boundary does see it, and that it queues a repair
// against an object that by then actually exists.
//
// OBSERVATION POINT: the repaired end state, NOT the MRF queue.
//
// Reading globalMRFState.opCh looks like the obvious way to see a queued repair,
// and it is wrong. prepareErasure -> initObjectLayer -> newErasureServerPools calls
// initAutoHeal (erasure-server-pool.go:197), which starts healRoutine, which drains
// opCh. A test that drains the channel itself is racing that consumer: an earlier
// version of this file "passed" by winning the race once and then failed under
// -shuffle, and its single observed op had in any case come from the pre-existing
// offline-disk hook rather than from Fix A.
//
// Asserting the end state is both sound and a stronger claim, because it exercises
// the whole chain -- detect, queue, drain, reconstruct -- rather than just the first
// link. It is also the in-process answer to "can a heal queued at the commit
// boundary actually repair a part-level shortfall", which is the question that
// decides whether heal mode is viable at all.

// completeOnePart drives a full single-part multipart upload. One part is used
// because globalMinPartSize applies to every part except the last, and a
// single-part upload's only part is its last.
func completeOnePart(ctx context.Context, t *testing.T, obj ObjectLayer, object string,
	arm func(), disarm func(),
) (ObjectInfo, error) {
	t.Helper()

	res, err := obj.NewMultipartUpload(ctx, "bucket", object, ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}

	if arm != nil {
		arm()
	}

	data := bytes.Repeat([]byte("a"), 1<<20)
	pi, err := obj.PutObjectPart(ctx, "bucket", object, res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("PutObjectPart should have been accepted, which is the defect under test: %v", err)
	}

	// Cleared before the commit so nothing CompleteMultipartUpload does is
	// attributed to the injected fault.
	if disarm != nil {
		disarm()
	}

	return obj.CompleteMultipartUpload(ctx, "bucket", object, res.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: pi.ETag}}, ObjectOptions{})
}

// removeUploadedPart deletes part.N and its .meta sidecar from ONE drive's
// staged multipart directory, and reports which drive it hit.
//
// Injecting via a failing RenamePart is how the fault arises in production, but it
// is not deterministic about the state it leaves: measured across shuffle orders it
// produces EITHER the object on 4 drives with the part on 3, which is the
// divergence, OR the object and the part both on 3, which is not a divergence at
// all and must not be reported. TestRenamePartShortfallIsSilent covers the
// mechanism; this constructs the state directly so the assertion is about Fix A's
// detection rather than about which outcome the injection happened to produce.
//
// Deleting the staged shard also matches the traced production shape more closely
// than a rename failure does: the shard was never written, and everything else
// about the upload is intact.
func removeUploadedPart(t *testing.T, fsDirs []string, part string) string {
	t.Helper()

	for _, dir := range fsDirs {
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
			continue
		}
		if err := os.Remove(metaPath); err != nil {
			t.Fatalf("removing %s: %v", metaPath, err)
		}
		// The shard itself sits next to its sidecar.
		if err := os.Remove(filepath.Join(filepath.Dir(metaPath), part)); err != nil {
			t.Fatalf("removing shard next to %s: %v", metaPath, err)
		}
		return dir
	}

	t.Fatalf("no staged %s.meta found under any of %d backing dirs", part, len(fsDirs))
	return ""
}

// stagedMetaRelPath converts the absolute path of a staged part.N.meta into the
// volume-relative path readParts expects, i.e. <uploadIDPath>/<dataDir>/part.N.meta
// under minioMetaMultipartBucket. Derived from the filesystem rather than rebuilt
// from getUploadIDDir so the test does not encode that layout.
func stagedMetaRelPath(t *testing.T, abs string) string {
	t.Helper()
	s := filepath.ToSlash(abs)
	i := strings.Index(s, minioMetaMultipartBucket+"/")
	if i < 0 {
		t.Fatalf("%q is not under %s", abs, minioMetaMultipartBucket)
	}
	return s[i+len(minioMetaMultipartBucket)+1:]
}

// findStagedMeta returns the absolute path of a staged part.N.meta on any drive.
func findStagedMeta(t *testing.T, fsDirs []string, part string) string {
	t.Helper()
	for _, dir := range fsDirs {
		var found string
		_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil || found != "" {
				return nil
			}
			if !d.IsDir() && d.Name() == part+".meta" &&
				strings.Contains(filepath.ToSlash(p), minioMetaMultipartBucket) {
				found = p
			}
			return nil
		})
		if found != "" {
			return found
		}
	}
	t.Fatalf("no staged %s.meta under any backing dir", part)
	return ""
}

// THE detection test, and it is fully deterministic: readParts is called directly,
// so nothing depends on the MRF queue, on healRoutine, or on which of the several
// healRoutines started by other tests happens to win.
//
// Asserts both directions. A part present everywhere must NOT be reported, and the
// same part missing from one drive MUST be, with the same call and the same disks.
func TestCommitSetReadPartsDetectsDivergence(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, _, fsDirs := setupRenamePartTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	res, err := obj.NewMultipartUpload(ctx, "bucket", "detect", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}
	data := bytes.Repeat([]byte("a"), 1<<20)
	if _, err := obj.PutObjectPart(ctx, "bucket", "detect", res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
		t.Fatalf("clean PutObjectPart: %v", err)
	}

	z := obj.(*erasureServerPools)
	disks := z.serverPools[0].sets[0].getDisks()
	rel := stagedMetaRelPath(t, findStagedMeta(t, fsDirs, "part.1"))

	// Clean: every drive holds it, so the sets agree.
	_, under, _, err := readParts(ctx, disks, minioMetaMultipartBucket, []string{rel}, []int{1}, 2)
	if err != nil {
		t.Fatalf("readParts on a clean upload: %v", err)
	}
	if len(under) != 0 {
		t.Fatalf("a part present on every drive must not be reported as divergent, got %v", under)
	}

	// The shard that never landed.
	victim := removeUploadedPart(t, fsDirs, "part.1")
	t.Logf("removed staged part.1 from %s", victim)

	_, under, _, err = readParts(ctx, disks, minioMetaMultipartBucket, []string{rel}, []int{1}, 2)
	if err != nil {
		t.Fatalf("readParts with one shard missing must still reach quorum: %v", err)
	}
	if len(under) != 1 || under[0] != 0 {
		t.Fatalf("a part held on 3 of 4 usable drives must be reported as divergent, got %v", under)
	}
	t.Log("readParts reported the divergence the part boundary cannot see")
}

// Whether a heal can actually repair this shape on THIS build, which is what
// decides if heal mode is viable at all. Called directly rather than through MRF so
// the result is about heal, not about queue scheduling.
//
// This matters because heal is known BROKEN for readable-but-incomplete objects on
// the deployed RELEASE.2024-08-26; upstream 16f8cf1c5 fixed it and HEAD has that
// fix. This is the check that HEAD really does.
func TestCommitSetHealRepairsPartShortfall(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, _, fsDirs := setupRenamePartTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	res, err := obj.NewMultipartUpload(ctx, "bucket", "healme", ObjectOptions{})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}
	data := bytes.Repeat([]byte("a"), 1<<20)
	pi, err := obj.PutObjectPart(ctx, "bucket", "healme", res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("clean PutObjectPart: %v", err)
	}

	removeUploadedPart(t, fsDirs, "part.1")

	if _, err := obj.CompleteMultipartUpload(ctx, "bucket", "healme", res.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: pi.ETag}}, ObjectOptions{}); err != nil {
		t.Fatalf("commit must succeed: %v", err)
	}

	xl := countCommittedObjectDirs(t, fsDirs, "xl.meta")
	parts := countCommittedObjectDirs(t, fsDirs, "part.1")
	t.Logf("after commit: xl.meta on %d/4 drives, part.1 on %d/4 drives", xl, parts)
	if xl == parts {
		t.Fatalf("no divergence produced: both on %d drives", xl)
	}

	if _, err := obj.HealObject(ctx, "bucket", "healme", "", madmin.HealOpts{
		ScanMode: madmin.HealNormalScan,
	}); err != nil {
		t.Fatalf("HealObject on a readable-but-incomplete object: %v", err)
	}

	if got := countCommittedObjectDirs(t, fsDirs, "part.1"); got != 4 {
		t.Fatalf("heal must restore part.1 to all 4 drives, got %d", got)
	}
	t.Log("heal repaired a part-level shortfall on this build")
}

// The control. Without this, a check that queued a heal unconditionally would pass
// the case above.
func TestCommitSetCleanUploadQueuesNothing(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	if _, err := completeOnePart(ctx, t, obj, "clean-commit", nil, nil); err != nil {
		t.Fatalf("clean upload must complete: %v", err)
	}
	if wrapped[0].calls.Load() == 0 {
		t.Fatal("the wrapper is not in the write path at all")
	}

	// A clean upload is already whole on every drive and must stay that way. If the
	// detection were inverted or unconditional this would still read 4, so the
	// discriminating control is the no-Fix-A run recorded in the results note.
	if got := countCommittedObjectDirs(t, fsDirs, "part.1"); got != 4 {
		t.Fatalf("clean upload must land part.1 on all 4 drives, got %d", got)
	}
	if got := countCommittedObjectDirs(t, fsDirs, "xl.meta"); got != 4 {
		t.Fatalf("clean upload must land xl.meta on all 4 drives, got %d", got)
	}
}

// A drive that is nil for the whole upload is excluded from the reference set by
// construction, so it must not be reported. This is the false-positive guard that
// the ORIGINAL Fix A criterion got wrong: comparing parts only against each other
// missed a drive that dropped every part, and comparing against a set that
// included offline drives would flag every degraded cluster on every commit.
func TestCommitSetOfflineDriveIsNotDivergence(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, w, fsDirs := setupOfflineWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	// Failing AND reporting itself offline for the whole upload. The part never
	// lands there, so every part's presence set is equally short and the sets agree.
	w.failing.Store(true)
	if _, err := completeOnePart(ctx, t, obj, "offline-drive", nil, nil); err != nil {
		t.Fatalf("upload must still complete with one drive down: %v", err)
	}
	if w.calls.Load() == 0 {
		t.Fatal("the injected drive was never asked to CreateFile")
	}

	// What the damage actually is, measured rather than assumed. xl.meta counts the
	// drives the committed object exists on; part.1 counts the drives that hold its
	// only shard. A gap between them IS the silent loss, whoever was supposed to
	// report it.
	xl := countCommittedObjectDirs(t, fsDirs, "xl.meta")
	parts := countCommittedObjectDirs(t, fsDirs, "part.1")
	t.Logf("after commit: xl.meta on %d/4 drives, part.1 on %d/4 drives", xl, parts)
	if xl == 4 && parts == 4 {
		t.Fatal("no shortfall was produced at all, so this case tests nothing")
	}
}

// countCommittedObjectDirs counts backing dirs holding `name` under the BUCKET
// path, i.e. after the commit rename, as opposed to countCommittedPartDirs which
// looks under .minio.sys/multipart before it.
func countCommittedObjectDirs(t *testing.T, fsDirs []string, name string) int {
	t.Helper()

	n := 0
	for _, dir := range fsDirs {
		found := false
		_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			s := filepath.ToSlash(p)
			if !d.IsDir() && d.Name() == name &&
				!strings.Contains(s, minioMetaBucket) {
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
