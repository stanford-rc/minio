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
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stanford-rc/minio/internal/logger"
)

// ELM 2026-10-17. Instrumentation for the partial usage figures the scanner
// publishes. A bucket's progress updates are partial sums: they carry the
// previous cycle's totals for prefixes not yet rescanned, and they leave out
// objects stored directly at the bucket's top level, which are added only
// when the root folder finishes. In the lab they also ran above the final
// total on a static bucket. The set's root cache holds the latest update while
// the bucket is being scanned, and the saver in erasureObjects.nsScanner
// publishes it on each save, every 30 to 40 seconds. The saver counts each
// published progress figure that the bucket's total then contradicts, logs a
// summary at the end of the cycle, and logs each bucket's case in detail at
// most daily.
//
// The informational lines go to the console logger, not to logger targets
// such as a webhook, and are not printed under --quiet.

// elmPartialThreshold is the relative difference between a published
// progress figure and the bucket's total above which the saver logs it.
const elmPartialThreshold = 0.01

// elmUsageDiffers reports whether any of size, objects or versions differ
// between a and b by more than elmPartialThreshold of the larger value.
func elmUsageDiffers(a, b dataUsageEntry) bool {
	return elmUsageDiff(a, b) > elmPartialThreshold
}

// elmUsageDiff returns the largest relative difference between a and b in
// size, objects or versions, each relative to the larger of the two values.
func elmUsageDiff(a, b dataUsageEntry) float64 {
	rel := func(x, y float64) float64 {
		hi := max(x, y)
		if hi == 0 {
			return 0
		}
		d := x - y
		if d < 0 {
			d = -d
		}
		return d / hi
	}
	return max(rel(float64(a.Size), float64(b.Size)),
		rel(float64(a.Objects), float64(b.Objects)),
		rel(float64(a.Versions), float64(b.Versions)))
}

// elmPartialSummary collects, over one cycle of one set, the published
// progress figures that their bucket's total contradicted.
type elmPartialSummary struct {
	n, above                   int
	worst                      float64
	worstName                  string
	worstPublished, worstTotal dataUsageEntry
}

// add records a published figure that the bucket's total contradicted.
func (s *elmPartialSummary) add(name string, published, total dataUsageEntry) {
	s.n++
	if published.Size > total.Size {
		s.above++
	}
	if d := elmUsageDiff(published, total); s.n == 1 || d > s.worst {
		s.worst, s.worstName, s.worstPublished, s.worstTotal = d, name, published, total
	}
}

// line returns the cycle's summary line, or "" if nothing was contradicted.
func (s *elmPartialSummary) line(cycle uint32) string {
	if s.n == 0 {
		return ""
	}
	return fmt.Sprintf("data-scanner: cycle %d: %d published progress figures were contradicted by their bucket's total, %d of them above it in size; largest difference %.1f%% for %s: published %s, total %s",
		cycle, s.n, s.above, 100*s.worst, s.worstName, elmUsageFigures(&s.worstPublished), elmUsageFigures(&s.worstTotal))
}

// elmPartialDetailInterval is how often a bucket's contradicted progress
// figure is logged in detail. The cycle's summary counts every case.
const elmPartialDetailInterval = 24 * time.Hour

var elmPartialDetails = struct {
	sync.Mutex
	last map[string]time.Time
}{last: map[string]time.Time{}}

// elmPartialDetailDue reports whether a contradicted progress figure for key,
// from elmScanKey, should be logged in detail now, and if so records it. The
// times are in memory, so a restart logs every bucket's next case.
func elmPartialDetailDue(key string, now time.Time) bool {
	elmPartialDetails.Lock()
	defer elmPartialDetails.Unlock()
	if last, ok := elmPartialDetails.last[key]; ok && now.Sub(last) < elmPartialDetailInterval {
		return false
	}
	elmPartialDetails.last[key] = now
	return true
}

// elmUsageFigures formats an entry's size, objects and versions for a log
// line, or "none" for a nil entry.
func elmUsageFigures(e *dataUsageEntry) string {
	if e == nil {
		return "none"
	}
	return fmt.Sprintf("size=%d objects=%d versions=%d", e.Size, e.Objects, e.Versions)
}

// elmScannerInfo logs an informational scanner line, at the level of the
// "IAM refresh took" notices, without the stack trace an error carries.
func elmScannerInfo(msg string, args ...any) {
	logger.Info(msg, args...)
}

// elmStreamFailureInterval is how often a full NSScanner stream-failure line
// is logged per remote drive. A node restarting fails every stream to its
// drives within seconds, and an isolated failure, such as the grid teardown
// race, is still logged in full.
const elmStreamFailureInterval = time.Minute

// elmStreamFailures limits storageRESTClient.NSScanner's failure log to one
// full line per remote drive per interval. It is keyed by the client's
// String(), the drive's endpoint, so a node with several drives gets a line
// for each; Elm runs one drive per node, so there the limit is also per node.
// Failures in between are counted by class and reported on the drive's next
// full line. Counts after a drive's last full line are reported only if it
// fails again; the scanner loop's per-drive summary still counts every
// failure.
var elmStreamFailures = struct {
	sync.Mutex
	peers map[string]*elmPeerFailures
}{peers: map[string]*elmPeerFailures{}}

type elmPeerFailures struct {
	last       time.Time
	suppressed map[string]int
}

// elmStreamFailureLine reports whether a failure of class on the remote drive
// peer, its endpoint, should be logged in full now. If so, it also returns a
// summary of the failures suppressed since that drive's last full line, or
// "none".
func elmStreamFailureLine(peer, class string, now time.Time) (bool, string) {
	elmStreamFailures.Lock()
	defer elmStreamFailures.Unlock()
	p := elmStreamFailures.peers[peer]
	if p == nil {
		p = &elmPeerFailures{suppressed: map[string]int{}}
		elmStreamFailures.peers[peer] = p
	}
	if !p.last.IsZero() && now.Sub(p.last) < elmStreamFailureInterval {
		p.suppressed[class]++
		return false, ""
	}
	summary := "none"
	if len(p.suppressed) > 0 {
		parts := make([]string, 0, len(p.suppressed))
		for c, n := range p.suppressed {
			parts = append(parts, fmt.Sprintf("%d x %s", n, c))
		}
		sort.Strings(parts)
		summary = strings.Join(parts, "; ")
		clear(p.suppressed)
	}
	p.last = now
	return true, summary
}

// elmNSScannerMessage returns the progress update to forward and the final to
// accept from one NSScanner stream message for bucket. A patched peer sets one
// field per message. A peer without the NSScannerHandler fix can set both, one
// of them stale from another stream's pooled response: a final message for
// this bucket carrying another bucket's update, or an update message carrying
// another bucket's final. So when both are set, the final's name decides which
// field is real. A final for another bucket is never accepted.
func elmNSScannerMessage(resp *nsScannerResp, bucket string) (update *dataUsageEntry, final *dataUsageCache) {
	own := resp.Final != nil && resp.Final.Info.Name == bucket
	if own {
		final = resp.Final
	}
	if resp.Update != nil && !own {
		update = resp.Update
	}
	return update, final
}

// A failed bucket scan leaves the bucket's previous entry in the root cache,
// so a bucket whose scan fails every cycle keeps its last successful figure
// while it grows. elmScanFailures counts each bucket's consecutive failed
// cycles so that a persistent failure is logged by name. A run is logged at
// elmScanFailureFirst failures and every elmScanFailureEvery after, and its
// end on the bucket's next successful scan. The counts are in memory, so a
// restart or a change of scanner leader starts every run again.
const (
	elmScanFailureFirst = 3
	elmScanFailureEvery = 100
)

var elmScanFailures = struct {
	sync.Mutex
	runs map[string]*elmScanRun
}{runs: map[string]*elmScanRun{}}

type elmScanRun struct {
	n     int
	cycle uint32
	since time.Time
}

// elmPendingScanFailure is a failed bucket scan held until the end of the
// drive's cycle, when elmDriveWide decides whether it extends the bucket's
// run.
type elmPendingScanFailure struct {
	bucket  string
	at      time.Time
	rootNow string
	err     error
}

// elmDriveWide reports whether a drive's failed bucket scans in one cycle
// point at the drive rather than at the buckets: at least two failures, and
// at least half of the buckets the drive took. The drives take buckets from a
// shared queue as they finish, so a drive scanning normally takes about its
// share and a broken bucket fails one of them. A drive failing fast takes
// most of the queue and fails all of it, and one failing slowly, by timeout,
// takes few buckets and fails all of those. A single failure is counted
// against its bucket, since it looks the same either way.
func elmDriveWide(failures, attempted int) bool {
	return failures >= 2 && 2*failures >= attempted
}

// elmScanKey identifies a bucket within its erasure set.
func elmScanKey(er erasureObjects, bucket string) string {
	return fmt.Sprintf("pool %d set %d %s", er.poolIndex, er.setIndex, bucket)
}

// elmScanFailed records a failed scan of key in cycle. It returns the run's
// length, the cycle and time of its first failure, and whether to log it now.
func elmScanFailed(key string, cycle uint32, now time.Time) (n int, firstCycle uint32, since time.Time, log bool) {
	elmScanFailures.Lock()
	defer elmScanFailures.Unlock()
	r := elmScanFailures.runs[key]
	if r == nil {
		r = &elmScanRun{cycle: cycle, since: now}
		elmScanFailures.runs[key] = r
	}
	r.n++
	log = r.n == elmScanFailureFirst || (r.n > elmScanFailureFirst && (r.n-elmScanFailureFirst)%elmScanFailureEvery == 0)
	return r.n, r.cycle, r.since, log
}

// elmScanSucceeded ends key's run of failed scans, if any, and returns its
// length and the time of its first failure.
func elmScanSucceeded(key string) (n int, since time.Time) {
	elmScanFailures.Lock()
	defer elmScanFailures.Unlock()
	r := elmScanFailures.runs[key]
	if r == nil {
		return 0, time.Time{}
	}
	delete(elmScanFailures.runs, key)
	return r.n, r.since
}
