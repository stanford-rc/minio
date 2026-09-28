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
	"context"
	"fmt"
	"testing"
)

// PRODUCER SIDE of the placement-validity guard.
//
// erasure-multipart-queuegate_test.go covers the consumer: given an invalid
// placement, unreadableAfter and the queue predicate must decline. Nothing
// covered the other half, that readParts actually SETS valid=false when it
// cannot map a drive onto a bitmask bit, and that it withholds underReplicated
// in the same breath.
//
// That coupling is the whole safety property. readParts reporting a shortfall
// while marking the placement invalid would hand the commit boundary a list of
// parts to act on with no way to judge whether acting is safe, which is how a
// below-quorum object gets queued for a heal that cannot help it.
//
// readParts touches exactly three methods on a drive, so the fake is small.
// Embedding StorageAPI leaves the rest nil: anything else readParts starts
// calling will panic here rather than silently pass, which is the behavior
// wanted from a guard test.
type placementDisk struct {
	StorageAPI
	diskIdx int
	online  bool
	parts   []*ObjectPartInfo
}

func (d *placementDisk) GetDiskLoc() (int, int, int) { return 0, 0, d.diskIdx }
func (d *placementDisk) IsOnline() bool              { return d.online }
func (d *placementDisk) String() string              { return fmt.Sprintf("disk%d", d.diskIdx) }

func (d *placementDisk) ReadParts(_ context.Context, _ string, paths ...string) ([]*ObjectPartInfo, error) {
	return d.parts, nil
}

// onePart is a drive holding part 1 under a shared ETag, or holding nothing.
func onePart(held bool) []*ObjectPartInfo {
	if !held {
		return []*ObjectPartInfo{nil}
	}
	return []*ObjectPartInfo{{Number: 1, ETag: "shared-etag", Size: 1}}
}

// threeOfFour builds the shape that MUST report a shortfall when tracking
// works: part 1 on drives 0,1,2 and absent from drive 3, every drive online.
// locs supplies each drive's within-set index, which is the only thing varied
// across these tests.
func threeOfFour(locs []int) []StorageAPI {
	disks := make([]StorageAPI, len(locs))
	for i, loc := range locs {
		disks[i] = &placementDisk{diskIdx: loc, online: true, parts: onePart(i != 3)}
	}
	return disks
}

func readPartsPlacement(t *testing.T, disks []StorageAPI) ([]int, partPlacement) {
	t.Helper()
	_, underReplicated, placement, err := readParts(
		context.Background(), disks, minioMetaMultipartBucket,
		[]string{"obj/uuid/part.1.meta"}, []int{1}, 3)
	if err != nil {
		t.Fatalf("readParts: %v", err)
	}
	return underReplicated, placement
}

func TestReadPartsTracksPlacementWhenEveryDiskLocIsInRange(t *testing.T) {
	// The control. Without it, a test asserting "no shortfall reported" proves
	// nothing, because a broken fake reports no shortfall too.
	underReplicated, placement := readPartsPlacement(t, threeOfFour([]int{0, 1, 2, 3}))

	if !placement.valid {
		t.Fatal("placement must be valid when every drive reports an in-range index")
	}
	if placement.usable != bit(0, 1, 2, 3) {
		t.Fatalf("usable = %#x, want %#x", placement.usable, bit(0, 1, 2, 3))
	}
	if len(placement.held) != 1 || placement.held[0] != bit(0, 1, 2) {
		t.Fatalf("held = %#x, want %#x", placement.held, bit(0, 1, 2))
	}
	if len(underReplicated) != 1 || underReplicated[0] != 0 {
		t.Fatalf("underReplicated = %v, want [0]: part 1 is on three of four usable drives",
			underReplicated)
	}
}

func TestReadPartsDisablesTrackingForAnUnassignedDiskLoc(t *testing.T) {
	// DiskIdx is -1 until SetDiskIndex runs during layout setup, so a negative
	// index means an endpoint that never got one. This is the only branch of the
	// guard reachable in a real deployment: setSizes caps a set at 16 drives, so
	// the >= 64 tests cannot fire.
	underReplicated, placement := readPartsPlacement(t, threeOfFour([]int{0, 1, -1, 3}))

	if placement.valid {
		t.Fatal("placement must be invalid when a drive reports an unassigned index")
	}
	// The coupling. Exactly the same damage as the control above, and the
	// shortfall must be WITHHELD rather than reported against a placement that
	// cannot be judged.
	if len(underReplicated) != 0 {
		t.Fatalf("underReplicated = %v, want none: reporting a shortfall against an "+
			"unjudgeable placement is what lets a lost object be queued blind",
			underReplicated)
	}
}

func TestReadPartsDisablesTrackingForAnOutOfRangeDiskLoc(t *testing.T) {
	// Unreachable today and asserted anyway: it is the branch that would fire if
	// anyone raised the 16-drive set cap past 64 without revisiting the bitmask.
	_, placement := readPartsPlacement(t, threeOfFour([]int{0, 1, 64, 3}))

	if placement.valid {
		t.Fatal("placement must be invalid for a within-set index at or above the bitmask width")
	}
}

func TestReadPartsLeavesMasksEmptyWhenTrackingIsDisabled(t *testing.T) {
	// An invalid placement must not carry half-populated masks. unreadableAfter
	// checks `valid` first so it would not read them, but a future caller that
	// forgets to is then reading a mask missing exactly the drives that caused
	// the bailout, which reads as data loss rather than as absent information.
	_, placement := readPartsPlacement(t, threeOfFour([]int{0, 1, -1, 3}))

	if placement.usable != 0 {
		t.Fatalf("usable = %#x, want 0 on an invalid placement", placement.usable)
	}
	for pidx, m := range placement.held {
		if m != 0 {
			t.Fatalf("held[%d] = %#x, want 0 on an invalid placement", pidx, m)
		}
	}
}

func TestReadPartsPlacementSurvivesAnOfflineDrive(t *testing.T) {
	// An offline drive is excluded from `usable` and must NOT disable tracking:
	// that is ordinary degraded operation, not a malformed layout. Conflating the
	// two would switch the commit-boundary checks off for the whole set every
	// time a node went down, which is precisely when they matter.
	disks := threeOfFour([]int{0, 1, 2, 3})
	disks[3].(*placementDisk).online = false

	underReplicated, placement := readPartsPlacement(t, disks)

	if !placement.valid {
		t.Fatal("an offline drive must not disable placement tracking")
	}
	if placement.usable != bit(0, 1, 2) {
		t.Fatalf("usable = %#x, want %#x: the offline drive is excluded",
			placement.usable, bit(0, 1, 2))
	}
	// Part 1 is on every drive that could serve this read, so there is no
	// shortfall to report even though one drive holds nothing.
	if len(underReplicated) != 0 {
		t.Fatalf("underReplicated = %v, want none: the missing drive was offline "+
			"and never had a chance to serve the read", underReplicated)
	}
}
