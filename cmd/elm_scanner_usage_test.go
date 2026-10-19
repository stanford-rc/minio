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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

func TestElmUsageDiffers(t *testing.T) {
	e := func(size int64, objects, versions uint64) dataUsageEntry {
		return dataUsageEntry{Size: size, Objects: objects, Versions: versions}
	}
	cases := []struct {
		name string
		a, b dataUsageEntry
		want bool
	}{
		{"identical", e(1000, 10, 12), e(1000, 10, 12), false},
		{"both empty", e(0, 0, 0), e(0, 0, 0), false},
		{"size within 1%", e(1000, 10, 12), e(1005, 10, 12), false},
		{"size over 1%", e(1000, 10, 12), e(1020, 10, 12), true},
		{"objects over 1%", e(1000, 100, 120), e(1000, 102, 120), true},
		{"versions over 1%", e(1000, 100, 100), e(1000, 100, 102), true},
		{"update empty, final not", e(0, 0, 0), e(1000, 10, 12), true},
		{"order does not matter", e(1020, 10, 12), e(1000, 10, 12), true},
	}
	for _, c := range cases {
		if got := elmUsageDiffers(c.a, c.b); got != c.want {
			t.Errorf("%s: elmUsageDiffers = %v, want %v", c.name, got, c.want)
		}
	}
	if got := elmUsageFigures(nil); got != "none" {
		t.Errorf("elmUsageFigures(nil) = %q, want none", got)
	}
}

// failingScanner fails the scan of one bucket after forwarding a progress
// update with a figure no bucket has, as a scan does when its stream fails
// after a stale or partial update was forwarded.
type failingScanner struct {
	StorageAPI
	bucket string
}

var errInjectedScan = errors.New("injected scan failure")

func (f failingScanner) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, mode madmin.HealScanMode, sleep func() bool) (dataUsageCache, error) {
	if cache.Info.Name != f.bucket {
		return f.StorageAPI.NSScanner(ctx, cache, updates, mode, sleep)
	}
	defer close(updates)
	select {
	case updates <- dataUsageEntry{Size: 987654321, Objects: 4321, Versions: 4321}:
	case <-ctx.Done():
		return cache, ctx.Err()
	}
	return cache, errInjectedScan
}

// elmScannerTestSet builds one four-drive set over local drives holding the
// named buckets, by default "target" and "other", of three objects each.
func elmScannerTestSet(t *testing.T, names ...string) (*erasureObjects, []BucketInfo) {
	t.Helper()
	ctx := t.Context()
	obj, disks, err := prepareErasure(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { removeRoots(disks) })
	if err := newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}
	initAllSubsystems(ctx)
	scannerSleeper.Update(0, 0)

	if len(names) == 0 {
		names = []string{"target", "other"}
	}
	for _, b := range names {
		if err := obj.MakeBucket(ctx, b, MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"d0/a", "d0/b", "d1/c"} {
			data := bytes.Repeat([]byte{'x'}, 100)
			if _, err := obj.PutObject(ctx, b, key, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	buckets, err := obj.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*erasureServerPools).serverPools[0].sets[0], buckets
}

// elmScannerTestCycle runs one scanner cycle over er and returns the last root
// cache it published.
func elmScannerTestCycle(ctx context.Context, er *erasureObjects, buckets []BucketInfo, want uint32) (dataUsageCache, error) {
	updates := make(chan dataUsageCache, 64)
	done := make(chan dataUsageCache)
	go func() {
		var last dataUsageCache
		for u := range updates {
			last = u
		}
		done <- last
	}()
	err := er.nsScanner(ctx, buckets, want, updates, madmin.HealNormalScan)
	close(updates)
	return <-done, err
}

// elmScanRunLength returns the length of bucket's current run of failed scans
// in er.
func elmScanRunLength(er *erasureObjects, bucket string) int {
	elmScanFailures.Lock()
	defer elmScanFailures.Unlock()
	if r := elmScanFailures.runs[elmScanKey(*er, bucket)]; r != nil {
		return r.n
	}
	return 0
}

// TestElmScannerErrorPathRestoresEntry runs a set's scanner over local drives.
// Cycles 2 to 4 fail one bucket's scan after it has forwarded a progress
// update. The root cache must then hold that bucket's entry from the first
// cycle, not the forwarded figure, and the bucket's run of failures must grow
// with each failed cycle and end with cycle 5's successful scan.
func TestElmScannerErrorPathRestoresEntry(t *testing.T) {
	elmScanFailures.Lock()
	elmScanFailures.runs = map[string]*elmScanRun{}
	elmScanFailures.Unlock()
	ctx := t.Context()
	er, buckets := elmScannerTestSet(t)
	cycle := func(want uint32) dataUsageCache {
		t.Helper()
		last, err := elmScannerTestCycle(ctx, er, buckets, want)
		if err != nil {
			t.Fatalf("cycle %d: %v", want, err)
		}
		return last
	}

	first := cycle(1)
	before := first.find("target")
	if before == nil || before.Objects != 3 {
		t.Fatalf("first cycle: target entry %v, want 3 objects", before)
	}

	real := er.getDisks
	er.getDisks = func() []StorageAPI {
		var wrapped []StorageAPI
		for _, d := range real() {
			if d != nil {
				d = failingScanner{StorageAPI: d, bucket: "target"}
			}
			wrapped = append(wrapped, d)
		}
		return wrapped
	}
	t.Cleanup(func() { er.getDisks = real })

	second := cycle(2)
	after := second.find("target")
	if after == nil {
		t.Fatal("second cycle: target missing from the root cache")
	}
	if after.Size != before.Size || after.Objects != before.Objects || after.Versions != before.Versions {
		t.Errorf("second cycle: target holds size=%d objects=%d versions=%d, want the first cycle's size=%d objects=%d versions=%d",
			after.Size, after.Objects, after.Versions, before.Size, before.Objects, before.Versions)
	}
	if o := second.find("other"); o == nil || o.Objects != 3 {
		t.Errorf("second cycle: other entry %v, want 3 objects", o)
	}

	// Each failed cycle extends target's run of failures, and the first
	// successful scan ends it.
	run := func(bucket string) int { return elmScanRunLength(er, bucket) }
	if n := run("target"); n != 1 {
		t.Errorf("after one failed cycle: target run %d, want 1", n)
	}
	cycle(3)
	fourth := cycle(4)
	if n := run("target"); n != 3 {
		t.Errorf("after three failed cycles: target run %d, want 3", n)
	}
	if n := run("other"); n != 0 {
		t.Errorf("other run %d, want 0", n)
	}
	if e := fourth.find("target"); e == nil || e.Objects != before.Objects {
		t.Errorf("fourth cycle: target entry %v, want the first cycle's %d objects", e, before.Objects)
	}
	er.getDisks = real
	cycle(5)
	if n := run("target"); n != 0 {
		t.Errorf("after a successful cycle: target run %d, want 0", n)
	}
}

// cancellingScanner cancels the cycle during one bucket's scan, after
// forwarding a progress update, as a leader change or shutdown does.
type cancellingScanner struct {
	StorageAPI
	bucket string
	cancel context.CancelFunc
}

func (c cancellingScanner) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, mode madmin.HealScanMode, sleep func() bool) (dataUsageCache, error) {
	if cache.Info.Name != c.bucket {
		return c.StorageAPI.NSScanner(ctx, cache, updates, mode, sleep)
	}
	defer close(updates)
	select {
	case updates <- dataUsageEntry{Size: 987654321, Objects: 4321, Versions: 4321}:
	case <-ctx.Done():
		return cache, ctx.Err()
	}
	c.cancel()
	<-ctx.Done()
	return cache, ctx.Err()
}

// TestElmScannerCancelledCycle cancels a cycle during one bucket's scan. The
// scan's error is the cancellation, not a failure of the bucket, so its run
// of failed scans must not grow. A failed scan in a later cycle still counts.
func TestElmScannerCancelledCycle(t *testing.T) {
	elmScanFailures.Lock()
	elmScanFailures.runs = map[string]*elmScanRun{}
	elmScanFailures.Unlock()
	er, buckets := elmScannerTestSet(t)
	if _, err := elmScannerTestCycle(t.Context(), er, buckets, 1); err != nil {
		t.Fatalf("cycle 1: %v", err)
	}

	real := er.getDisks
	t.Cleanup(func() { er.getDisks = real })
	wrap := func(f func(StorageAPI) StorageAPI) {
		er.getDisks = func() []StorageAPI {
			var wrapped []StorageAPI
			for _, d := range real() {
				if d != nil {
					d = f(d)
				}
				wrapped = append(wrapped, d)
			}
			return wrapped
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wrap(func(d StorageAPI) StorageAPI {
		return cancellingScanner{StorageAPI: d, bucket: "target", cancel: cancel}
	})
	_, err := elmScannerTestCycle(ctx, er, buckets, 2)
	t.Logf("cancelled cycle returned %v", err)
	if ctx.Err() == nil {
		t.Fatal("the cycle was not cancelled")
	}
	if n := elmScanRunLength(er, "target"); n != 0 {
		t.Errorf("after a cancelled cycle: target run %d, want 0", n)
	}

	wrap(func(d StorageAPI) StorageAPI { return failingScanner{StorageAPI: d, bucket: "target"} })
	if _, err := elmScannerTestCycle(t.Context(), er, buckets, 3); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}
	if n := elmScanRunLength(er, "target"); n != 1 {
		t.Errorf("after a failed cycle: target run %d, want 1", n)
	}
}

func TestElmPartialSummary(t *testing.T) {
	e := func(size int64, objects uint64) dataUsageEntry {
		return dataUsageEntry{Size: size, Objects: objects, Versions: objects}
	}
	var s elmPartialSummary
	if got := s.line(7); got != "" {
		t.Errorf("empty summary: line %q, want none", got)
	}
	s.add("a", e(1100, 10), e(1000, 10)) // above, 9.1%
	s.add("b", e(500, 10), e(1000, 10))  // below, 50%
	s.add("c", e(1000, 10), e(1000, 20)) // objects only, 50%, not the first at 50%
	want := "data-scanner: cycle 7: 3 published progress figures were contradicted by their bucket's total, 1 of them above it in size; largest difference 50.0% for b: published size=500 objects=10 versions=10, total size=1000 objects=10 versions=10"
	if got := s.line(7); got != want {
		t.Errorf("line:\n got %q\nwant %q", got, want)
	}
	if d := elmUsageDiff(e(1000, 10), e(1000, 40)); d != 0.75 {
		t.Errorf("elmUsageDiff on objects = %v, want 0.75", d)
	}
}

func TestElmPartialDetailDue(t *testing.T) {
	elmPartialDetails.Lock()
	elmPartialDetails.last = map[string]time.Time{}
	elmPartialDetails.Unlock()

	t0 := time.Unix(1_800_000_000, 0)
	check := func(key string, at time.Duration, want bool) {
		t.Helper()
		if got := elmPartialDetailDue(key, t0.Add(at)); got != want {
			t.Errorf("%s at +%s: got %v, want %v", key, at, got, want)
		}
	}
	check("k", 0, true)
	check("k", time.Hour, false)
	check("other", time.Hour, true) // keys are independent
	check("k", elmPartialDetailInterval-time.Second, false)
	check("k", elmPartialDetailInterval, true)
	check("k", elmPartialDetailInterval+time.Hour, false)
}

// brokenDrive fails every bucket's scan at once, as a drive that is reachable
// but not serving does. slowDrive delays each scan, so that the drives
// scanning normally take few buckets from the queue while brokenDrive takes
// the rest.
type brokenDrive struct{ StorageAPI }

func (b brokenDrive) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, mode madmin.HealScanMode, sleep func() bool) (dataUsageCache, error) {
	close(updates)
	return cache, errInjectedScan
}

type slowDrive struct{ StorageAPI }

func (s slowDrive) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, mode madmin.HealScanMode, sleep func() bool) (dataUsageCache, error) {
	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
	}
	return s.StorageAPI.NSScanner(ctx, cache, updates, mode, sleep)
}

// TestElmScannerDriveWideFailures runs three cycles in which one drive fails
// every scan it takes. Those failures point at the drive, so no bucket's run
// of failed scans may grow.
func TestElmScannerDriveWideFailures(t *testing.T) {
	elmScanFailures.Lock()
	elmScanFailures.runs = map[string]*elmScanRun{}
	elmScanFailures.Unlock()
	names := []string{"dw-0", "dw-1", "dw-2", "dw-3", "dw-4", "dw-5", "dw-6", "dw-7"}
	er, buckets := elmScannerTestSet(t, names...)
	real := er.getDisks
	t.Cleanup(func() { er.getDisks = real })
	er.getDisks = func() []StorageAPI {
		var wrapped []StorageAPI
		for i, d := range real() {
			switch {
			case d == nil:
			case i == 0:
				d = brokenDrive{d}
			default:
				d = slowDrive{d}
			}
			wrapped = append(wrapped, d)
		}
		return wrapped
	}
	for c := uint32(1); c <= 3; c++ {
		if _, err := elmScannerTestCycle(t.Context(), er, buckets, c); err != nil {
			t.Fatalf("cycle %d: %v", c, err)
		}
	}
	for _, b := range names {
		if n := elmScanRunLength(er, b); n != 0 {
			t.Errorf("%s: run %d after three drive-wide cycles, want 0", b, n)
		}
	}
}

func TestElmDriveWide(t *testing.T) {
	cases := []struct {
		failures, attempted int
		want                bool
	}{
		{0, 50, false},
		{1, 1, false}, // one failure counts against its bucket
		{1, 50, false},
		{2, 50, false}, // two broken buckets on one drive
		{2, 4, true},
		{3, 3, true}, // a drive timing out on every scan
		{188, 188, true},
		{20, 50, false}, // a drive lost partway through the cycle
	}
	for _, c := range cases {
		if got := elmDriveWide(c.failures, c.attempted); got != c.want {
			t.Errorf("elmDriveWide(%d, %d) = %v, want %v", c.failures, c.attempted, got, c.want)
		}
	}
}

func TestElmNSScannerMessage(t *testing.T) {
	up := &dataUsageEntry{Size: 1111}
	own := &dataUsageCache{Info: dataUsageCacheInfo{Name: "a"}}
	other := &dataUsageCache{Info: dataUsageCacheInfo{Name: "b"}}
	cases := []struct {
		name       string
		resp       nsScannerResp
		wantUpdate *dataUsageEntry
		wantFinal  *dataUsageCache
	}{
		{"update alone", nsScannerResp{Update: up}, up, nil},
		{"own final alone", nsScannerResp{Final: own}, nil, own},
		{"other final alone", nsScannerResp{Final: other}, nil, nil},
		{"own final with a stale update", nsScannerResp{Update: up, Final: own}, nil, own},
		{"update with a stale final", nsScannerResp{Update: up, Final: other}, up, nil},
		{"empty", nsScannerResp{}, nil, nil},
	}
	for _, c := range cases {
		u, f := elmNSScannerMessage(&c.resp, "a")
		if u != c.wantUpdate || f != c.wantFinal {
			t.Errorf("%s: got (%v, %v), want (%v, %v)", c.name, u, f, c.wantUpdate, c.wantFinal)
		}
	}
}

func TestElmScanFailed(t *testing.T) {
	elmScanFailures.Lock()
	elmScanFailures.runs = map[string]*elmScanRun{}
	elmScanFailures.Unlock()

	t0 := time.Unix(1_800_000_000, 0)
	var logged []int
	for i := 1; i <= 2*elmScanFailureEvery+elmScanFailureFirst; i++ {
		n, firstCycle, since, log := elmScanFailed("k", uint32(10+i), t0.Add(time.Duration(i)*time.Minute))
		if n != i || firstCycle != 11 || !since.Equal(t0.Add(time.Minute)) {
			t.Fatalf("failure %d: got (%d, %d, %s), want (%d, 11, %s)", i, n, firstCycle, since, i, t0.Add(time.Minute))
		}
		if log {
			logged = append(logged, n)
		}
	}
	want := []int{elmScanFailureFirst, elmScanFailureFirst + elmScanFailureEvery, elmScanFailureFirst + 2*elmScanFailureEvery}
	if fmt.Sprint(logged) != fmt.Sprint(want) {
		t.Errorf("logged at %v, want %v", logged, want)
	}
	if n, _, _, _ := elmScanFailed("other", 1, t0); n != 1 {
		t.Errorf("other key: run %d, want 1", n) // keys are independent
	}
	if n, since := elmScanSucceeded("k"); n != 2*elmScanFailureEvery+elmScanFailureFirst || !since.Equal(t0.Add(time.Minute)) {
		t.Errorf("elmScanSucceeded = (%d, %s), want (%d, %s)", n, since, 2*elmScanFailureEvery+elmScanFailureFirst, t0.Add(time.Minute))
	}
	if n, _ := elmScanSucceeded("k"); n != 0 {
		t.Errorf("second elmScanSucceeded: run %d, want 0", n)
	}
	if n, _, _, _ := elmScanFailed("k", 99, t0); n != 1 {
		t.Errorf("after success: run %d, want 1", n)
	}
}

func TestElmStreamFailureLine(t *testing.T) {
	elmStreamFailures.Lock()
	elmStreamFailures.peers = map[string]*elmPeerFailures{}
	elmStreamFailures.Unlock()

	t0 := time.Unix(1_800_000_000, 0)
	check := func(peer, class string, at time.Duration, wantFull bool, wantSummary string) {
		t.Helper()
		full, summary := elmStreamFailureLine(peer, class, t0.Add(at))
		if full != wantFull || summary != wantSummary {
			t.Errorf("%s %s at +%s: got (%v, %q), want (%v, %q)", peer, class, at, full, summary, wantFull, wantSummary)
		}
	}
	check("p1", "a", 0, true, "none")
	check("p1", "a", time.Second, false, "")
	check("p1", "b", 2*time.Second, false, "")
	check("p1", "a", 3*time.Second, false, "")
	check("p2", "a", 4*time.Second, true, "none") // peers are independent
	check("p1", "c", time.Minute, true, "1 x b; 2 x a")
	check("p1", "c", time.Minute+time.Second, false, "")
	check("p1", "c", 3*time.Minute, true, "1 x c")
}
