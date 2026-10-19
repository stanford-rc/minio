// Copyright (c) 2026 Stanford Research Computing.
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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/minio/madmin-go/v3"
)

// crossBucketUsage is what the data-usage roll-up should report for one
// bucket: every version's bytes, distinct keys, and versions.
type crossBucketUsage struct {
	size     uint64
	objects  uint64
	versions uint64
}

func (u crossBucketUsage) String() string {
	return fmt.Sprintf("size=%d objects=%d versions=%d", u.size, u.objects, u.versions)
}

// TestDataUsageCrossBucket drives the whole roll-up, erasureServerPools.
// NSScanner over one 4-drive erasure set, the layout of Elm's tiers, and fails
// if any bucket in any update carries another bucket's exact figures.
//
// All four drives are local, so the roll-up never goes through the grid or
// its response pool, and this test cannot reach the production fault. It is a
// check that the roll-up logic itself does not copy one bucket's figures to
// another. TestStorageRESTNSScannerNoStaleFields covers the grid path.
//
// In production on 2026-10-16 an empty bucket reported a large bucket's size,
// object count, version count and both histograms to the byte, in .usage.json
// and in the set's root .usage-cache.bin, while the empty bucket's own
// per-bucket cache was correct. Every bucket here has a unique (size, objects,
// versions), so a copy cannot be mistaken for a coincidence, and some buckets
// stay empty as that one was.
//
// Writes land between cycles, so the figures keep changing as they did in
// production. Run it under -race; CROSSBUCKET_CYCLES raises the cycle count.
func TestDataUsageCrossBucket(t *testing.T) {
	ctx := t.Context()
	obj, disks, err := prepareErasure(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(disks)
	if err := newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}
	initAllSubsystems(ctx)
	// No throttling between objects, so a cycle takes milliseconds.
	scannerSleeper.Update(0, 0)

	cycles := 40
	if v, err := strconv.Atoi(os.Getenv("CROSSBUCKET_CYCLES")); err == nil && v > 0 {
		cycles = v
	}
	rng := rand.New(rand.NewSource(20261016))

	const nBuckets = 24
	want := map[string]crossBucketUsage{}
	keys := map[string]int{}
	put := func(bucket string, key string, size int) {
		t.Helper()
		data := bytes.Repeat([]byte{'x'}, size)
		_, err := obj.PutObject(ctx, bucket, key,
			mustGetPutObjReader(t, bytes.NewReader(data), int64(size), "", ""),
			ObjectOptions{Versioned: true})
		if err != nil {
			t.Fatalf("put %s/%s: %v", bucket, key, err)
		}
		u := want[bucket]
		u.size += uint64(size)
		u.versions++
		if _, seen := keys[bucket+"/"+key]; !seen {
			u.objects++
			keys[bucket+"/"+key] = 1
		}
		want[bucket] = u
	}

	var buckets []string
	for i := range nBuckets {
		b := fmt.Sprintf("bucket-%02d", i)
		if err := obj.MakeBucket(ctx, b, MakeBucketOptions{VersioningEnabled: true}); err != nil {
			t.Fatal(err)
		}
		buckets = append(buckets, b)
		want[b] = crossBucketUsage{}
		// Every fourth bucket stays empty, as the production victim was.
		if i%4 == 3 {
			continue
		}
		// One large bucket, so its walk overlaps the others as the
		// production source's did.
		n := 2*i + 1
		if i == 0 {
			n = 1500
		}
		for j := range n {
			// Sizes unique per bucket: 1000*(i+1) + j bytes.
			put(b, fmt.Sprintf("dir%d/obj%04d", j%7, j), 1000*(i+1)+j)
		}
		// A second version of the first key, so versions != objects.
		put(b, "dir0/obj0000", 1000*(i+1)+7)
	}

	var stale int
	check := func(cycle uint32, dui DataUsageInfo, final bool) {
		t.Helper()
		got := map[string]crossBucketUsage{}
		for b, bu := range dui.BucketsUsage {
			got[b] = crossBucketUsage{bu.Size, bu.ObjectsCount, bu.VersionsCount}
		}
		for _, a := range buckets {
			g := got[a]
			if g == want[a] {
				continue
			}
			for _, b := range buckets {
				if b == a || (want[b] == crossBucketUsage{}) {
					continue
				}
				// A bucket missing from this update has no figure to
				// copy, so got[b] counts only when b is present.
				gb, present := got[b]
				if g == want[b] || (present && g == gb) {
					t.Errorf("cycle %d: %s reports %s, which is %s's figure (own: %s)",
						cycle, a, g, b, want[a])
				}
			}
			// The scanner revisits an unchanged folder only every
			// dataUsageUpdateDirCycles, so a recent write can lag. That
			// is stale, not a copy: tolerated while writes continue, and
			// a failure once the quiet cycles have converged.
			if final {
				t.Errorf("cycle %d: %s reports %s, want %s", cycle, a, g, want[a])
			} else {
				stale++
			}
		}
	}

	// Writes stop after `cycles`; the quiet cycles after that must converge
	// every bucket to its exact figure.
	quiet := uint32(dataUsageUpdateDirCycles + 1)
	for cycle := uint32(1); cycle <= uint32(cycles)+quiet; cycle++ {
		writing := cycle <= uint32(cycles)
		updates := make(chan DataUsageInfo, 1)
		var last DataUsageInfo
		var n int
		done := make(chan struct{})
		go func() {
			defer close(done)
			for dui := range updates {
				n++
				last = dui
				check(cycle, dui, false)
			}
		}()
		if err := obj.NSScanner(ctx, updates, cycle, madmin.HealNormalScan); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		<-done
		if n == 0 {
			t.Fatalf("cycle %d: no usage update", cycle)
		}
		check(cycle, last, cycle == uint32(cycles)+quiet)
		if t.Failed() {
			return
		}
		if !writing {
			continue
		}

		// Activity between cycles: new versions in a few random buckets,
		// never in the empty ones.
		for range 3 {
			i := rng.Intn(nBuckets)
			if i%4 == 3 {
				continue
			}
			put(buckets[i], fmt.Sprintf("new/c%03d-%d", cycle, i), 1000*(i+1)+500+int(cycle))
		}
	}
	t.Logf("%d cycles, %d stale bucket reports while writing, no cross-bucket copy",
		uint32(cycles)+quiet, stale)
}
