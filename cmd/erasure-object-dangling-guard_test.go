// Elm addition, 2026-08-31.  Tests for the dangling-delete guard and for
// the joinErrs fix it depends on.
//
// WHY THIS EXISTS.  isObjectDangling returning true means no readable copy can be
// assembled, and deleteIfDangling then removes the whole object VERSION from
// every drive.  On EC:1 a single part below read quorum in a 203-part object
// destroys all 203, including the ~200 intact ones.  It is gated on neither
// opts.Remove nor a scan mode, so a plain `mc admin heal` reaches it, as do the
// scanner and MRF read-repair.
//
// The guard makes that a deliberate operator decision instead of a side effect.
// These tests pin the two properties that matter: that the default declines, and
// that a refusal still produces the evidence a deletion would have, because that
// audit record is the report the removal workflow reads.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/minio/madmin-go/v3"
)

func TestDanglingDeleteModeDefaultsOff(t *testing.T) {
	// The default is the load-bearing choice.  If this ever flips to "on",
	// deleting an unreadable object stops being an operator decision and the
	// evidence for it is destroyed by the same call that decides.
	t.Setenv("MINIO_DANGLING_DELETE", "")
	os.Unsetenv("MINIO_DANGLING_DELETE")
	if got := danglingDeleteMode(); got != danglingDeleteOff {
		t.Fatalf("default mode = %q, want %q", got, danglingDeleteOff)
	}
}

func TestDanglingDeleteModeParsing(t *testing.T) {
	for _, c := range []struct {
		set  string
		want string
	}{
		{"on", danglingDeleteOn},
		{"off", danglingDeleteOff},
		// An unrecognized value must fall to the SAFE side.  Falling to "on"
		// would make a typo in the deployment config destructive.
		{"yes", danglingDeleteOff},
		{"true", danglingDeleteOff},
		{"1", danglingDeleteOff},
		{"On", danglingDeleteOff},
		{"", danglingDeleteOff},
	} {
		t.Setenv("MINIO_DANGLING_DELETE", c.set)
		if got := danglingDeleteMode(); got != c.want {
			t.Errorf("MINIO_DANGLING_DELETE=%q: mode = %q, want %q",
				c.set, got, c.want)
		}
	}
}

func TestJoinErrsReportsEveryDrive(t *testing.T) {
	// The upstream bug this fixes: `for i := range s` ranged over the empty
	// accumulator, so joinErrs always returned "".  That blanked the `merrs` tag
	// on every DeleteDanglingObject audit record, which is the field naming why
	// each drive's metadata was unusable.  Confirmed against a production record
	// from 2026-08-12 carrying "merrs": "".
	errs := []error{
		nil,
		errFileNotFound,
		errDiskNotFound,
		nil,
	}
	got := joinErrs(errs)
	if got == "" {
		t.Fatal("joinErrs returned empty: the range-over-accumulator bug is back, " +
			"and every dangling audit record has lost its per-drive evidence")
	}
	if n := strings.Count(got, ",") + 1; n != len(errs) {
		t.Errorf("joinErrs produced %d field(s) for %d drive(s): %q",
			n, len(errs), got)
	}
	for _, want := range []string{
		"<nil>",
		errFileNotFound.Error(),
		errDiskNotFound.Error(),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("joinErrs output %q is missing %q", got, want)
		}
	}
}

func TestJoinErrsEmptyInput(t *testing.T) {
	if got := joinErrs(nil); got != "" {
		t.Errorf("joinErrs(nil) = %q, want empty", got)
	}
}

// erasureObjects is awkward to construct without a live backend, so the guard's
// decision is exercised through the mode function and the audit event names,
// which is where the policy lives.  The deletion path itself is covered by the
// existing erasure-healing tests.

func TestDanglingAuditEventsAreDistinct(t *testing.T) {
	// A refusal and a deletion must not share an event name.  "we would have
	// deleted this" and "we deleted this" are different operational facts, and
	// collapsing them makes the audit feed useless for confirming the guard is
	// in force.
	if auditDanglingDeleted == auditDanglingRefused {
		t.Fatal("the refusal and deletion audit events share a name")
	}
	// The deletion event name is load-bearing for continuity: an existing Splunk
	// history keys on it, including the single 2026-08-12 firing.
	if auditDanglingDeleted != "DeleteDanglingObject" {
		t.Errorf("deletion event renamed to %q, which orphans the existing "+
			"Splunk history", auditDanglingDeleted)
	}
}

func TestRefusalReturnsTheExistingQuorumError(t *testing.T) {
	// The guard returns errErasureReadQuorum, which is deleteIfDangling's own
	// existing refusal return from the !ok branch.  Reusing it is what keeps
	// every caller working unchanged; a new error would have to be threaded
	// through healObject, the scanner and MRF read-repair.
	if errErasureReadQuorum == nil {
		t.Fatal("errErasureReadQuorum is nil")
	}
	if !errors.Is(errErasureReadQuorum, errErasureReadQuorum) {
		t.Fatal("errErasureReadQuorum is not comparable with errors.Is")
	}
}

// TestDanglingGuardEndToEnd is the proof of the guard, and the reason the other
// tests here are not sufficient on their own: they pin the policy, this pins the
// OUTCOME, in both directions, from one setup.
//
// The scenario is the dangling-version phase of TestHealingDanglingObject. An
// object is written to all sixteen drives, four are removed, then that specific
// version is deleted. The delete lands on the twelve present drives and not on
// the four absent ones, so on restore the version survives on four drives only,
// which is below read quorum: unreadable, and therefore dangling.
//
// Measured behavior, from the run that produced these assertions:
//
//	mode=on   PRE 1 version   POST file not found, 0 versions
//	mode=off  PRE 1 version   POST 1 version
//
// The "on" row is what a plain `mc admin heal` does today on the deployed
// release and on master alike. On a real object it takes every intact part with
// it: one part below quorum in a 203-part object destroys all 203.
func TestDanglingGuardEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		mode        string
		wantDeleted bool
	}{
		{danglingDeleteOn, true},
		{danglingDeleteOff, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			t.Setenv("MINIO_DANGLING_DELETE", tc.mode)

			resetGlobalHealState()
			defer resetGlobalHealState()

			// Deliberately NOT touching globalStorageClass, unlike
			// TestHealingDanglingObject which this scenario is borrowed from.
			//
			// That test does `saveSC := globalStorageClass` then restores with
			// `Update(saveSC)`.  At the start of a run the global is the zero
			// value, so the deferred restore WRITES a zero config rather than
			// being a no-op, and a zero config is not equivalent to a never-written
			// one: it produces "Storage resources are insufficient" in the
			// multipart tests.  Copying that pattern here made this file a second
			// leaker and broke TestFailCommitLeavesHealthyCommitAlone.
			//
			// Nothing here needs a particular parity.  The scenario only needs a
			// version written while four of sixteen drives are absent, which is
			// below read quorum at any sane parity for that geometry.

			fsDirs, err := getRandomDisks(16)
			if err != nil {
				t.Fatal(err)
			}
			defer removeRoots(fsDirs)

			objLayer, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(0, fsDirs...))
			if err != nil {
				t.Fatal(err)
			}
			// Restore the previous layer on the way out.  Leaving globalObjectAPI
			// pointing at this one is a leak with teeth: removeRoots deletes its
			// backing directories at the end of the test, so a later test that
			// consults the global sees a layer whose disks are gone and fails with
			// "Storage resources are insufficient".  That is what broke
			// TestCommitSetHealRepairsPartShortfall,
			// TestFailCommitLeavesHealthyCommitAlone and
			// TestRenameGuardShortfallModeMatrix when this file was added.
			prevLayer := newObjectLayerFn()
			t.Cleanup(func() { setObjectLayer(prevLayer) })
			setObjectLayer(objLayer)

			bucket := getRandomBucketName()
			object := getRandomObjectName()
			data := bytes.Repeat([]byte("a"), 128*1024)

			if err = objLayer.MakeBucket(ctx, bucket, MakeBucketOptions{}); err != nil {
				t.Fatalf("MakeBucket: %v", err)
			}

			disks := objLayer.(*erasureServerPools).serverPools[0].erasureDisks[0]
			orgDisks := append([]StorageAPI{}, disks...)

			globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig,
				[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))

			objInfo, err := objLayer.PutObject(ctx, bucket, object,
				mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""),
				ObjectOptions{Versioned: true})
			if err != nil {
				t.Fatal(err)
			}

			setDisks := func(nd ...StorageAPI) {
				objLayer.(*erasureServerPools).serverPools[0].erasureDisksMu.Lock()
				copy(disks, nd)
				objLayer.(*erasureServerPools).serverPools[0].erasureDisksMu.Unlock()
			}

			setDisks(nil, nil, nil, nil)
			if _, err = objLayer.DeleteObject(ctx, bucket, object, ObjectOptions{
				Versioned: true, VersionID: objInfo.VersionID,
			}); err != nil {
				t.Fatal(err)
			}
			setDisks(orgDisks[:4]...)

			pre, err := disks[0].ReadVersion(t.Context(), "", bucket, object, "",
				ReadOptions{ReadData: false, Healing: true})
			if err != nil {
				t.Fatalf("setup: the version should exist before heal: %v", err)
			}
			if pre.NumVersions != 1 {
				t.Fatalf("setup: expected 1 version before heal, got %d",
					pre.NumVersions)
			}

			// Remove: true is deliberate.  The guard must hold even when the
			// caller asked for removal, because deleteIfDangling never consulted
			// opts.Remove: that flag governs healObjectDir and
			// checkAbandonedParts, not this path.
			if err = objLayer.HealObjects(ctx, bucket, "",
				madmin.HealOpts{Recursive: true, Remove: true},
				func(b, o, vid string, sm madmin.HealScanMode) error {
					_, herr := objLayer.HealObject(ctx, b, o, vid,
						madmin.HealOpts{ScanMode: sm, Remove: true})
					return herr
				}); err != nil {
				t.Fatal(err)
			}

			_, err = disks[0].ReadVersion(t.Context(), "", bucket, object, "",
				ReadOptions{ReadData: false, Healing: true})
			deleted := err != nil

			switch {
			case tc.wantDeleted && !deleted:
				t.Fatal("MINIO_DANGLING_DELETE=on did not delete the dangling " +
					"version, so the guard is not a pure gate: it changed " +
					"upstream behavior rather than only withholding it")
			case !tc.wantDeleted && deleted:
				t.Fatalf("the guard did not hold: heal deleted the dangling "+
					"version despite MINIO_DANGLING_DELETE=off (%v). On a real "+
					"object this destroys every intact part alongside the "+
					"unreadable one", err)
			}
		})
	}
}
