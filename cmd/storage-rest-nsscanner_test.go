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
	"context"
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/minio/madmin-go/v3"
)

// scriptedScanner stands in for the remote drive's NSScanner, so the test
// controls exactly which update and final messages each bucket's stream
// carries. Everything else is the real drive.
type scriptedScanner struct {
	StorageAPI
}

// scriptedSize gives each bucket a figure no other bucket shares.
var scriptedSize = map[string]uint64{"bucket-a": 1111, "bucket-b": 0, "bucket-c": 3333}

func (s scriptedScanner) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, _ madmin.HealScanMode, _ func() bool) (dataUsageCache, error) {
	defer close(updates)
	name := cache.Info.Name
	e := dataUsageEntry{Size: int64(scriptedSize[name])}
	// bucket-b is empty, as the production victim was, and sends no
	// progress update: its stream is one final message.
	if name != "bucket-b" {
		select {
		case updates <- e:
		case <-ctx.Done():
			return cache, ctx.Err()
		}
	}
	cache.replace(name, "", e)
	return cache, nil
}

// TestStorageRESTNSScannerNoStaleFields drives NSScannerHandler over a real
// two-host grid and checks every message a client receives. Each message
// should carry an Update or a Final, never both, and an Update should carry
// only the figure of the bucket whose stream it is on.
//
// The handler takes responses from the grid's pool, which never clears them.
// If the handler sets only the field it is sending, a pooled response keeps
// the other field from an earlier stream. A final message for an empty bucket
// then carries another bucket's progress update, and
// storageRESTClient.NSScanner forwards that update under the empty bucket's
// name. That is the shape of the 2026-10-16 production fault, in which an
// empty bucket reported a large bucket's figures to the byte.
//
// Before each stream the pool is seeded with dirty responses, so the test does
// not depend on sync.Pool happening to reuse one. Each seed has its own Update
// and Final, so that a regression shows as stale fields rather than as a data
// race between seeds under -race. NSSCANNER_STRESS, if set, gives the number
// of rounds per loop in place of the default 200.
func TestStorageRESTNSScannerNoStaleFields(t *testing.T) {
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

	seed := func() {
		for range 32 {
			u := dataUsageEntry{Size: 9999}
			f := dataUsageCache{Info: dataUsageCacheInfo{Name: "bucket-z"}}
			storageNSScannerRPC.PutResponse(&nsScannerResp{Update: &u, Final: &f})
		}
	}

	rounds := 200
	if n, _ := strconv.Atoi(os.Getenv("NSSCANNER_STRESS")); n > 0 {
		rounds = n
	}
	// Streams that end in context.Canceled hit the grid teardown race, which
	// -race makes common and TestStorageRESTNSScannerStreamEndErrors covers.
	// Their messages are still checked here. In the raw loop they are split by
	// whether the bucket's own final reached the callback, since the race can
	// also drop a final that was still buffered.
	var both, foreign, forwarded, messages int
	var racedAfterFinal, racedNoFinal, racedClient int
	for round := range rounds {
		for _, bucket := range []string{"bucket-a", "bucket-b", "bucket-c"} {
			seed()
			cache := dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}
			st, err := storageNSScannerRPC.Call(t.Context(), client.gridConn, &nsScannerOptions{
				DiskID: *client.diskID.Load(),
				Cache:  &cache,
			})
			if err != nil {
				t.Fatalf("round %d %s: call: %v", round, bucket, err)
			}
			var finals, ownFinals int
			err = st.Results(func(resp *nsScannerResp) error {
				messages++
				if resp.Update != nil && resp.Final != nil {
					both++
					if both <= 3 {
						t.Errorf("round %d %s: message carries both an Update (size %d) and a Final (%q)",
							round, bucket, resp.Update.Size, resp.Final.Info.Name)
					}
				}
				if resp.Update != nil && uint64(resp.Update.Size) != scriptedSize[bucket] {
					foreign++
					if foreign <= 3 {
						t.Errorf("round %d %s: Update has size %d, another bucket's figure (own %d)",
							round, bucket, resp.Update.Size, scriptedSize[bucket])
					}
				}
				if resp.Final != nil {
					finals++
					if resp.Final.Info.Name == bucket {
						ownFinals++
					}
				}
				return nil
			})
			if errors.Is(err, context.Canceled) {
				if ownFinals > 0 {
					racedAfterFinal++
				} else {
					racedNoFinal++
				}
				continue
			}
			if err != nil {
				t.Fatalf("round %d %s: results: %v", round, bucket, err)
			}
			if finals == 0 {
				t.Fatalf("round %d %s: no final message", round, bucket)
			}
		}
	}

	// The same sequence through the client the scanner actually uses: the
	// empty bucket must receive no update at all.
	for round := range rounds {
		for _, bucket := range []string{"bucket-a", "bucket-b", "bucket-c"} {
			seed()
			updates := make(chan dataUsageEntry, 4)
			done := make(chan []dataUsageEntry)
			go func() {
				var got []dataUsageEntry
				for u := range updates {
					got = append(got, u)
				}
				done <- got
			}()
			final, err := client.NSScanner(t.Context(), dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}}, updates, madmin.HealNormalScan, nil)
			got := <-done
			if errors.Is(err, context.Canceled) {
				racedClient++
			} else if err != nil {
				t.Fatalf("round %d %s: NSScanner: %v", round, bucket, err)
			}
			if final.Info.Name != bucket {
				t.Errorf("round %d %s: final cache is named %q", round, bucket, final.Info.Name)
			}
			for _, u := range got {
				if uint64(u.Size) != scriptedSize[bucket] {
					forwarded++
					if forwarded <= 3 {
						t.Errorf("round %d %s: client forwarded an update of size %d under this bucket's name (own %d)",
							round, bucket, u.Size, scriptedSize[bucket])
					}
				}
			}
		}
	}
	t.Logf("raw: %d messages, %d carried both fields, %d updates carried another bucket's figure, %d streams ended in context.Canceled after their own final and %d without it; client: %d updates forwarded under the wrong bucket, %d calls ended in context.Canceled",
		messages, both, foreign, racedAfterFinal, racedNoFinal, forwarded, racedClient)
}
