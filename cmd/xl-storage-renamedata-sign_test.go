// Copyright (c) 2015-2026 MinIO, Inc.
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
	"slices"
	"testing"
)

// RenameDataResp.Sign is always empty, and REPAIRING IT PANICS
// CompleteMultipartUpload and PutObject on the very fault the code it feeds was
// written to handle. Order of work matters here, so read this before touching
// either the producer or the consumer.
//
// THE PRODUCER, cmd/xl-storage.go in RenameData:
//
//	dst := []byte{}
//	for _, ver := range xlMeta.versions {
//		dst = slices.Grow(dst, 16)
//		copy(dst[len(dst):], ver.header.VersionID[:])
//	}
//	res.Sign = dst
//
// slices.Grow raises CAPACITY and leaves LENGTH alone, so dst[len(dst):] is a
// zero-length slice on every iteration and copy moves zero bytes. The loop reads
// as though it concatenates every VersionID. It concatenates nothing.
// TestRenameDataSignCopyIdiom pins that.
//
// THE CONSUMER, reduceCommonVersions in cmd/erasure-metadata-utils.go. Its FIRST
// loop guards with len(versions) > 0. Its SECOND loop does not:
//
//	if maxCnt >= writeQuorum {
//		for _, versions := range diskVersions {
//			if binary.BigEndian.Uint64(versions) == commonVersions {
//
// binary.BigEndian.Uint64 does _ = b[7] and panics on anything shorter than 8
// bytes. renameData builds diskVersions as make([][]byte, len(disks)) and only
// assigns on success, so any drive whose RenameData errored leaves a nil entry.
//
// THE HAZARD. With Sign repaired: quorum met, plus a nil entry positioned ahead
// of the first match, equals a panic. Measured by
// TestReduceCommonVersionsPanicsOnNilAheadOfMatch: with four drives, three
// agreeing and one failed, it panics when and only when the failed drive sits at
// slice index 0, so one upload in four.
//
// That input is not hypothetical. A drive that fails renameData while write
// quorum still holds is exactly what produces a nil entry alongside a met
// quorum, and it is the fault reproduced on 2026-09-15 for the commit-set
// shortfall work in cmd/erasure-multipart.go.
//
// CORRECT ORDER OF WORK, three separate changes:
//
//  1. guard the second loop in reduceCommonVersions
//  2. then repair the Sign copy
//  3. then decide, on its own merits, whether a per-upload MRF heal is wanted
//
// Step 3 is a real capacity question rather than an obvious yes. Today Sign
// being empty means `versions` is always empty, so in
// CompleteMultipartUpload and putObject:
//
//	len(versions) > 0  -> addPartialOp(...)                  never runs
//	len(versions) == 0 -> disk==nil||!IsOnline -> addPartial  always runs
//
// Repairing Sign swaps those: an MRF op for EVERY completed upload, and the
// drive-specific hook stops running. On a tape-backed tier with a 5 GiB minimum
// part size the upload rate may be low enough that a post-write verification
// heal on everything is actually desirable. That is worth evaluating, not
// foreclosing. What must not happen is reaching step 2 or 3 before step 1.
//
// WHAT `versions` MEANS IS GENUINELY UNSETTLED UPSTREAM, so do not lean on
// either comment. The producer says, for the >10 versions case, "avoid healing
// such objects in this manner, let it heal during the regular scanner cycle",
// which reads as though the per-version MRF heal is the INTENDED primary path
// for <=10 versions. The consumer in putObject says "When there is versions
// disparity we are healing the content implicitly for all versions", i.e.
// non-empty means DISPARITY. But reduceCommonVersions' own doc comment says it
// returns empty when "quorum cannot be achieved and disks have too many
// inconsistent versions", i.e. non-empty means AGREEMENT. The two cannot both
// be right. Recording the contradiction is more useful than picking a side.
//
// SCOPE IS NOT MULTIPART-ONLY despite this file's name. putObject
// (cmd/erasure-object.go) carries the identical construct in if/else form, so
// single-part PUTs are affected the same way and the panic applies there too.
// The tests below cover both because they exercise the shared helpers.
//
// NOT A BUG, so leave it alone: healRoutine's uuid.UUID(u.Versions[16*i:]) in
// cmd/mrf.go has no upper slice bound and looks wrong. Go's slice-to-array
// conversion takes the first 16 bytes and panics only if the slice is shorter,
// so it is correct as written.

// TestRenameDataSignCopyIdiom pins the Go semantics that make Sign empty, so the
// claim rests on a running assertion rather than on reading the loop.
func TestRenameDataSignCopyIdiom(t *testing.T) {
	versionIDs := [][16]byte{
		{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11},
		{0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22},
	}

	// verbatim from RenameData
	dst := []byte{}
	for _, id := range versionIDs {
		dst = slices.Grow(dst, 16)
		copy(dst[len(dst):], id[:])
	}

	if len(dst) != 0 {
		t.Fatalf("the Sign idiom now produces %d bytes (%x), so the copy was repaired. "+
			"Confirm the second loop in reduceCommonVersions was guarded FIRST, or "+
			"CompleteMultipartUpload panics on a single failed drive. Read the comment "+
			"at the top of this file", len(dst), dst)
	}

	// the direct proof: copy itself reports moving nothing
	probe := slices.Grow([]byte{}, 16)
	if n := copy(probe[len(probe):], versionIDs[0][:]); n != 0 {
		t.Fatalf("copy into a zero-length slice moved %d bytes, expected 0", n)
	}

	// what the loop reads as if it did, kept so the intent is unambiguous
	var want []byte
	for _, id := range versionIDs {
		want = append(want, id[:]...)
	}
	if len(want) != len(versionIDs)*16 {
		t.Fatalf("append idiom produced %d bytes, expected %d", len(want), len(versionIDs)*16)
	}
}

// TestReduceCommonVersionsPanicsOnNilAheadOfMatch is the hazard, pinned. It
// asserts the CURRENT unguarded behaviour on purpose: if someone guards the
// second loop, this test fails and says so, which is the signal that repairing
// Sign has become safe.
func TestReduceCommonVersionsPanicsOnNilAheadOfMatch(t *testing.T) {
	sign := make([]byte, 16)
	for i := range sign {
		sign[i] = 0xAB
	}

	// Four drives, three agreeing, one failed leaving a nil entry. Quorum is
	// met, so the second loop runs and hits the nil before any match.
	panicked := func(pos int) (p bool) {
		dv := [][]byte{sign, sign, sign, sign}
		dv[pos] = nil
		defer func() {
			if recover() != nil {
				p = true
			}
		}()
		reduceCommonVersions(dv, 3)
		return
	}

	if !panicked(0) {
		t.Fatal("reduceCommonVersions no longer panics with a nil entry at index 0. " +
			"If the second loop was guarded, that is the fix this file asks for: " +
			"update this test and see step 2 of the order of work above")
	}

	// Positions after the first match are unreachable, because the loop returns
	// on index 0. This is why the failure is one upload in four rather than
	// every upload with a failed drive.
	for pos := 1; pos < 4; pos++ {
		if panicked(pos) {
			t.Errorf("unexpected panic with nil at index %d; the loop should have "+
				"returned on the match at index 0", pos)
		}
	}
}

// TestReduceCommonVersionsOnEmptySigns pins the downstream half of the Sign
// defect: even with every drive reporting and full quorum, empty Signs cannot
// yield a non-empty result, so `versions` is always empty.
func TestReduceCommonVersionsOnEmptySigns(t *testing.T) {
	for _, tc := range []struct {
		name         string
		diskVersions [][]byte
		writeQuorum  int
	}{
		{
			name:         "four drives, all empty as RenameData produces them",
			diskVersions: [][]byte{{}, {}, {}, {}},
			writeQuorum:  3,
		},
		{
			name:         "nil and empty mixed, one drive absent",
			diskVersions: [][]byte{{}, nil, {}, {}},
			writeQuorum:  3,
		},
		{
			name:         "quorum of one, still empty",
			diskVersions: [][]byte{{}},
			writeQuorum:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reduceCommonVersions(tc.diskVersions, tc.writeQuorum); len(got) != 0 {
				t.Fatalf("reduceCommonVersions returned %d bytes (%x), expected empty; "+
					"the len(versions) == 0 branch in CompleteMultipartUpload and "+
					"putObject would stop running", len(got), got)
			}
		})
	}
}

// TestReduceCommonVersionsOnPopulatedSigns is the control. It shows the function
// is not vacuously empty, so the result above is attributable to the Sign
// producer rather than to reduceCommonVersions. This is also the behaviour that
// would take effect once the copy idiom is repaired.
func TestReduceCommonVersionsOnPopulatedSigns(t *testing.T) {
	sign := make([]byte, 16)
	for i := range sign {
		sign[i] = 0xAB
	}

	got := reduceCommonVersions([][]byte{sign, sign, sign, sign}, 3)
	if len(got) != 16 {
		t.Fatalf("with four agreeing 16-byte Signs and quorum 3, got %d bytes (%x), expected 16",
			len(got), got)
	}

	// Below quorum returns empty, which is the documented contract. Note the
	// empties sit AFTER the single Sign: maxCnt is 1 against quorum 3, so the
	// second loop never runs and this case cannot reach the panic. That near
	// miss is why TestReduceCommonVersionsPanicsOnNilAheadOfMatch exists.
	if got := reduceCommonVersions([][]byte{sign, {}, {}, {}}, 3); len(got) != 0 {
		t.Fatalf("with one Sign against quorum 3, got %d bytes (%x), expected empty", len(got), got)
	}
}
