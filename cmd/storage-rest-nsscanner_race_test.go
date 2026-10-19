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
	"os"
	"sort"
	"strconv"
	"testing"
)

// TestStorageRESTNSScannerStreamEndErrors runs many short NSScanner streams
// over a real two-host grid, with nothing failing, and counts the streams
// whose Results returned an error after their own final message had arrived. Any
// such error sends the scanner down its error path, where the bucket's
// corrected total is never sent.
//
// It is a stress test, skipped unless NSSCANNER_RACE_STRESS gives the number
// of streams to run. Under load it fails: the grid teardown race in
// muxClient.close() is not fixed in this fork, by decision, since with the
// NSScannerHandler fix and the scanner's restore of the previous entry a race
// hit costs the bucket one cycle's figure. It has its own variable, apart from
// the NSSCANNER_STRESS that TestStorageRESTNSScannerNoStaleFields uses, so a
// soak of that test does not fail on the race. Errors before the final are
// reported too: the race can also cancel a stream while its final is still
// buffered, and that final is then dropped.
func TestStorageRESTNSScannerStreamEndErrors(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("NSSCANNER_RACE_STRESS"))
	if n <= 0 {
		t.Skip("set NSSCANNER_RACE_STRESS=<streams> to run")
	}
	client := newStorageRESTHTTPServerClient(t)

	globalLocalDrivesMu.Lock()
	real := globalLocalSetDrives[0][0][0]
	globalLocalSetDrives[0][0][0] = scriptedScanner{real}
	globalLocalDrivesMu.Unlock()
	t.Cleanup(func() {
		globalLocalDrivesMu.Lock()
		globalLocalSetDrives[0][0][0] = real
		globalLocalDrivesMu.Unlock()
	})

	afterFinal := map[string]int{}
	beforeFinal := map[string]int{}
	for i := range n {
		// bucket-b sends no update, so its stream is one final message,
		// like the 28 ms production scan that kept another bucket's figure.
		bucket := []string{"bucket-a", "bucket-b", "bucket-c"}[i%3]
		cache := dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}
		st, err := storageNSScannerRPC.Call(t.Context(), client.gridConn, &nsScannerOptions{
			DiskID: *client.diskID.Load(),
			Cache:  &cache,
		})
		if err != nil {
			t.Fatalf("stream %d: call: %v", i, err)
		}
		var final bool
		err = st.Results(func(resp *nsScannerResp) error {
			// A stale Final from another bucket's stream can ride on an
			// update message, so only this bucket's own final counts.
			if resp.Final != nil && resp.Final.Info.Name == bucket {
				final = true
			}
			return nil
		})
		if err != nil {
			k := fmt.Sprintf("%T: %v", err, err)
			if final {
				afterFinal[k]++
			} else {
				beforeFinal[k]++
			}
		}
	}
	report := func(name string, m map[string]int) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			t.Logf("%s: %d x %s", name, m[k], k)
		}
	}
	report("error after final", afterFinal)
	report("error before final", beforeFinal)
	total := 0
	for _, v := range afterFinal {
		total += v
	}
	t.Logf("%d streams, %d ended in an error after the final arrived", n, total)
	if total > 0 {
		t.Errorf("%d of %d streams returned an error after delivering the final message", total, n)
	}
}
