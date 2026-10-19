// Copyright (c) 2015-2021 MinIO, Inc.
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
	"fmt"
	"maps"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/stanford-rc/minio/internal/dsync"
	xioutil "github.com/stanford-rc/minio/internal/ioutil"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// erasureObjects - Implements ER object layer.
type erasureObjects struct {
	setDriveCount      int
	defaultParityCount int

	setIndex  int
	poolIndex int

	// getDisks returns list of storageAPIs.
	getDisks func() []StorageAPI

	// getLockers returns list of remote and local lockers.
	getLockers func() ([]dsync.NetLocker, string)

	// getEndpoints returns list of endpoint belonging this set.
	// some may be local and some remote.
	getEndpoints func() []Endpoint

	// getEndpoints returns list of endpoint strings belonging this set.
	// some may be local and some remote.
	getEndpointStrings func() []string

	// Locker mutex map.
	nsMutex *nsLockMap
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (er erasureObjects) NewNSLock(bucket string, objects ...string) RWLocker {
	return er.nsMutex.NewNSLock(er.getLockers, bucket, objects...)
}

// Shutdown function for object storage interface.
func (er erasureObjects) Shutdown(ctx context.Context) error {
	// Add any object layer shutdown activities here.
	closeStorageDisks(er.getDisks()...)
	return nil
}

// defaultWQuorum write quorum based on setDriveCount and defaultParityCount
func (er erasureObjects) defaultWQuorum() int {
	dataCount := er.setDriveCount - er.defaultParityCount
	if dataCount == er.defaultParityCount {
		return dataCount + 1
	}
	return dataCount
}

// defaultRQuorum read quorum based on setDriveCount and defaultParityCount
func (er erasureObjects) defaultRQuorum() int {
	return er.setDriveCount - er.defaultParityCount
}

func diskErrToDriveState(err error) (state string) {
	switch {
	case errors.Is(err, errDiskNotFound) || errors.Is(err, context.DeadlineExceeded):
		state = madmin.DriveStateOffline
	case errors.Is(err, errCorruptedFormat) || errors.Is(err, errCorruptedBackend):
		state = madmin.DriveStateCorrupt
	case errors.Is(err, errUnformattedDisk):
		state = madmin.DriveStateUnformatted
	case errors.Is(err, errDiskAccessDenied):
		state = madmin.DriveStatePermission
	case errors.Is(err, errFaultyDisk):
		state = madmin.DriveStateFaulty
	case errors.Is(err, errDriveIsRoot):
		state = madmin.DriveStateRootMount
	case err == nil:
		state = madmin.DriveStateOk
	default:
		state = fmt.Sprintf("%s (cause: %s)", madmin.DriveStateUnknown, err)
	}

	return state
}

func getOnlineOfflineDisksStats(disksInfo []madmin.Disk) (onlineDisks, offlineDisks madmin.BackendDisks) {
	onlineDisks = make(madmin.BackendDisks)
	offlineDisks = make(madmin.BackendDisks)

	for _, disk := range disksInfo {
		ep := disk.Endpoint
		if _, ok := offlineDisks[ep]; !ok {
			offlineDisks[ep] = 0
		}
		if _, ok := onlineDisks[ep]; !ok {
			onlineDisks[ep] = 0
		}
	}

	// Wait for the routines.
	for _, disk := range disksInfo {
		ep := disk.Endpoint
		state := disk.State
		if state != madmin.DriveStateOk && state != madmin.DriveStateUnformatted {
			offlineDisks[ep]++
			continue
		}
		onlineDisks[ep]++
	}

	rootDiskCount := 0
	for _, di := range disksInfo {
		if di.RootDisk {
			rootDiskCount++
		}
	}

	// Count offline disks as well to ensure consistent
	// reportability of offline drives on local setups.
	if len(disksInfo) == (rootDiskCount + offlineDisks.Sum()) {
		// Success.
		return onlineDisks, offlineDisks
	}

	// Root disk should be considered offline
	for i := range disksInfo {
		ep := disksInfo[i].Endpoint
		if disksInfo[i].RootDisk {
			offlineDisks[ep]++
			onlineDisks[ep]--
		}
	}

	return onlineDisks, offlineDisks
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI, endpoints []Endpoint, metrics bool) (disksInfo []madmin.Disk) {
	disksInfo = make([]madmin.Disk, len(disks))

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		g.Go(func() error {
			di := madmin.Disk{
				Endpoint:  endpoints[index].String(),
				PoolIndex: endpoints[index].PoolIdx,
				SetIndex:  endpoints[index].SetIdx,
				DiskIndex: endpoints[index].DiskIdx,
				Local:     endpoints[index].IsLocal,
			}
			if disks[index] == OfflineDisk {
				di.State = diskErrToDriveState(errDiskNotFound)
				disksInfo[index] = di
				return nil
			}
			info, err := disks[index].DiskInfo(context.TODO(), DiskInfoOptions{Metrics: metrics})
			di.DrivePath = info.MountPath
			di.TotalSpace = info.Total
			di.UsedSpace = info.Used
			di.AvailableSpace = info.Free
			di.UUID = info.ID
			di.Major = info.Major
			di.Minor = info.Minor
			di.RootDisk = info.RootDisk
			di.Healing = info.Healing
			di.Scanning = info.Scanning
			di.State = diskErrToDriveState(err)
			di.FreeInodes = info.FreeInodes
			di.UsedInodes = info.UsedInodes
			if hi := disks[index].Healing(); hi != nil {
				hd := hi.toHealingDisk()
				di.HealInfo = &hd
			}
			di.Metrics = &madmin.DiskMetrics{
				LastMinute:              make(map[string]madmin.TimedAction, len(info.Metrics.LastMinute)),
				APICalls:                make(map[string]uint64, len(info.Metrics.APICalls)),
				TotalErrorsAvailability: info.Metrics.TotalErrorsAvailability,
				TotalErrorsTimeout:      info.Metrics.TotalErrorsTimeout,
				TotalWaiting:            info.Metrics.TotalWaiting,
			}
			for k, v := range info.Metrics.LastMinute {
				if v.N > 0 {
					di.Metrics.LastMinute[k] = v.asTimedAction()
				}
			}
			maps.Copy(di.Metrics.APICalls, info.Metrics.APICalls)
			if info.Total > 0 {
				di.Utilization = float64(info.Used / info.Total * 100)
			}
			disksInfo[index] = di
			return nil
		}, index)
	}

	g.Wait()
	return disksInfo
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI, endpoints []Endpoint, metrics bool) StorageInfo {
	disksInfo := getDisksInfo(disks, endpoints, metrics)

	// Sort so that the first element is the smallest.
	sort.Slice(disksInfo, func(i, j int) bool {
		return disksInfo[i].TotalSpace < disksInfo[j].TotalSpace
	})

	storageInfo := StorageInfo{
		Disks: disksInfo,
	}

	storageInfo.Backend.Type = madmin.Erasure
	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (er erasureObjects) StorageInfo(ctx context.Context) StorageInfo {
	disks := er.getDisks()
	endpoints := er.getEndpoints()
	return getStorageInfo(disks, endpoints, true)
}

// LocalStorageInfo - returns underlying local storage statistics.
func (er erasureObjects) LocalStorageInfo(ctx context.Context, metrics bool) StorageInfo {
	disks := er.getDisks()
	endpoints := er.getEndpoints()

	var localDisks []StorageAPI
	var localEndpoints []Endpoint

	for i, endpoint := range endpoints {
		if endpoint.IsLocal {
			localDisks = append(localDisks, disks[i])
			localEndpoints = append(localEndpoints, endpoint)
		}
	}

	return getStorageInfo(localDisks, localEndpoints, metrics)
}

// getOnlineDisksWithHealingAndInfo - returns online disks and overall healing status.
// Disks are ordered in the following groups:
// - Non-scanning disks
// - Non-healing disks
// - Healing disks (if inclHealing is true)
func (er erasureObjects) getOnlineDisksWithHealingAndInfo(inclHealing bool) (newDisks []StorageAPI, newInfos []DiskInfo, healing int) {
	var wg sync.WaitGroup
	disks := er.getDisks()
	infos := make([]DiskInfo, len(disks))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, i := range r.Perm(len(disks)) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			disk := disks[i]
			if disk == nil {
				infos[i].Error = errDiskNotFound.Error()
				return
			}

			di, err := disk.DiskInfo(context.Background(), DiskInfoOptions{})
			infos[i] = di
			if err != nil {
				// - Do not consume disks which are not reachable
				//   unformatted or simply not accessible for some reason.
				infos[i].Error = err.Error()
			}
		}()
	}
	wg.Wait()

	var scanningDisks, healingDisks []StorageAPI
	var scanningInfos, healingInfos []DiskInfo

	for i, info := range infos {
		// Check if one of the drives in the set is being healed.
		// this information is used by scanner to skip healing
		// this erasure set while it calculates the usage.
		if info.Error != "" || disks[i] == nil {
			continue
		}
		if info.Healing {
			healing++
			if inclHealing {
				healingDisks = append(healingDisks, disks[i])
				healingInfos = append(healingInfos, infos[i])
			}
			continue
		}

		if !info.Scanning {
			newDisks = append(newDisks, disks[i])
			newInfos = append(newInfos, infos[i])
		} else {
			scanningDisks = append(scanningDisks, disks[i])
			scanningInfos = append(scanningInfos, infos[i])
		}
	}

	// Prefer non-scanning disks over disks which are currently being scanned.
	newDisks = append(newDisks, scanningDisks...)
	newInfos = append(newInfos, scanningInfos...)

	/// Then add healing disks.
	newDisks = append(newDisks, healingDisks...)
	newInfos = append(newInfos, healingInfos...)

	return newDisks, newInfos, healing
}

func (er erasureObjects) getOnlineDisksWithHealing(inclHealing bool) ([]StorageAPI, bool) {
	newDisks, _, healing := er.getOnlineDisksWithHealingAndInfo(inclHealing)
	return newDisks, healing > 0
}

// Clean-up previously deleted objects. from .minio.sys/tmp/.trash/
func (er erasureObjects) cleanupDeletedObjects(ctx context.Context) {
	var wg sync.WaitGroup
	for _, disk := range er.getLocalDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(disk StorageAPI) {
			defer wg.Done()
			drivePath := disk.Endpoint().Path
			readDirFn(pathJoin(drivePath, minioMetaTmpDeletedBucket), func(ddir string, typ os.FileMode) error {
				w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
				return w.Run(func() error {
					wait := deleteCleanupSleeper.Timer(ctx)
					removeAll(pathJoin(drivePath, minioMetaTmpDeletedBucket, ddir))
					wait()
					return nil
				})
			})
		}(disk)
	}
	wg.Wait()
}

// nsScanner will start scanning buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (er erasureObjects) nsScanner(ctx context.Context, buckets []BucketInfo, wantCycle uint32, updates chan<- dataUsageCache, healScanMode madmin.HealScanMode) error {
	if len(buckets) == 0 {
		return nil
	}

	// Collect disks we can use.
	disks, healing := er.getOnlineDisksWithHealing(false)
	if len(disks) == 0 {
		scannerLogIf(ctx, errors.New("data-scanner: all drives are offline or being healed, skipping scanner cycle"))
		return nil
	}

	// Load bucket totals
	oldCache := dataUsageCache{}
	if err := oldCache.load(ctx, er, dataUsageCacheName); err != nil {
		return err
	}

	// New cache..
	cache := dataUsageCache{
		Info: dataUsageCacheInfo{
			Name:      dataUsageRoot,
			NextCycle: oldCache.Info.NextCycle,
		},
		Cache: make(map[string]dataUsageEntry, len(oldCache.Cache)),
	}

	// Put all buckets into channel.
	bucketCh := make(chan BucketInfo, len(buckets))

	// Shuffle buckets to ensure total randomness of buckets, being scanned.
	// Otherwise same set of buckets get scanned across erasure sets always.
	// at any given point in time. This allows different buckets to be scanned
	// in different order per erasure set, this wider spread is needed when
	// there are lots of buckets with different order of objects in them.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	permutes := r.Perm(len(buckets))
	// Add new buckets first
	for _, idx := range permutes {
		b := buckets[idx]
		if e := oldCache.find(b.Name); e == nil {
			bucketCh <- b
		}
	}
	for _, idx := range permutes {
		b := buckets[idx]
		if e := oldCache.find(b.Name); e != nil {
			cache.replace(b.Name, dataUsageRoot, *e)
			bucketCh <- b
		}
	}
	xioutil.SafeClose(bucketCh)

	bucketResults := make(chan dataUsageEntryInfo, len(disks))

	// Start async collector/saver.
	// This goroutine owns the cache.
	var saverWg sync.WaitGroup
	saverWg.Add(1)
	go func() {
		// Add jitter to the update time so multiple sets don't sync up.
		updateTime := 30*time.Second + time.Duration(float64(10*time.Second)*rand.Float64())
		t := time.NewTicker(updateTime)
		defer t.Stop()
		defer saverWg.Done()
		var lastSave time.Time
		// ELM 2026-10-17. Progress figures by bucket: the latest one the
		// root cache holds, and the one the last save published. A bucket's
		// entries leave both maps when its final total or restored entry
		// arrives.
		partial := map[string]dataUsageEntry{}
		published := map[string]dataUsageEntry{}
		var contradicted elmPartialSummary

		for {
			select {
			case <-t.C:
				if cache.Info.LastUpdate.Equal(lastSave) {
					continue
				}
				scannerLogOnceIf(ctx, cache.save(ctx, er, dataUsageCacheName), "nsscanner-cache-update")
				updates <- cache.clone()
				for name, e := range partial {
					published[name] = e
				}

				lastSave = cache.Info.LastUpdate
			case v, ok := <-bucketResults:
				if !ok {
					// ELM 2026-10-17. A bucket still here kept a progress
					// figure as its total: a failed scan with no previous
					// entry to restore, or a cancelled cycle.
					if len(partial) > 0 {
						names := make([]string, 0, min(len(partial), 10))
						for name := range partial {
							if len(names) == cap(names) {
								break
							}
							names = append(names, name)
						}
						elmScannerInfo("data-scanner: cycle %d ended with a progress figure as the total for %d buckets, including %q",
							wantCycle, len(partial), names)
					}
					if line := contradicted.line(wantCycle); line != "" {
						elmScannerInfo("%s", line)
					}
					// Save final state...
					cache.Info.NextCycle = wantCycle
					cache.Info.LastUpdate = time.Now()
					scannerLogOnceIf(ctx, cache.save(ctx, er, dataUsageCacheName), "nsscanner-channel-closed")
					updates <- cache.clone()
					return
				}
				// ELM 2026-10-17. Count each published progress figure that
				// the bucket's total then contradicted, for the cycle's
				// summary, and log it in detail at most daily per bucket.
				if v.Partial {
					partial[v.Name] = v.Entry
				} else {
					if p, ok := published[v.Name]; ok && elmUsageDiffers(p, v.Entry) {
						contradicted.add(v.Name, p, v.Entry)
						if elmPartialDetailDue(elmScanKey(er, v.Name), time.Now()) {
							what := "final"
							if v.Restored {
								what = "scan failed, previous entry restored"
							}
							elmScannerInfo("data-scanner: cycle %d: a root-cache save published %s for %s; %s %s, top level %s (logged at most daily per bucket)",
								wantCycle, elmUsageFigures(&p), v.Name, what, elmUsageFigures(&v.Entry), elmUsageFigures(v.TopLevel))
						}
					}
					delete(partial, v.Name)
					delete(published, v.Name)
				}
				cache.replace(v.Name, v.Parent, v.Entry)
				cache.Info.LastUpdate = time.Now()
			}
		}
	}()

	// Restrict parallelism for disk usage scanner
	// upto GOMAXPROCS if GOMAXPROCS is < len(disks)
	maxProcs := runtime.GOMAXPROCS(0)
	if maxProcs < len(disks) {
		disks = disks[:maxProcs]
	}

	// Start one scanner per disk
	var wg sync.WaitGroup
	wg.Add(len(disks))

	for i := range disks {
		go func(i int) {
			defer wg.Done()
			disk := disks[i]

			// ELM 2026-10-17. A drive that drops out mid-cycle fails every
			// remaining bucket at once, so only its first failure in a
			// cycle is logged in full and the rest are summed on exit.
			// A cancelled cycle is not a failure of the bucket being
			// scanned, and is reported once, as information.
			// Failures extend their buckets' runs of failed scans only
			// when the cycle ends, and only if the drive's failures were
			// not drive-wide; see elmDriveWide.
			var attempted, failures, failuresForwarded int
			var lastFailure error
			var furtherFailed []string // up to 10 names, after the first failure
			var cancelledDuring string
			var runFailures []elmPendingScanFailure
			defer func() {
				driveWide := elmDriveWide(failures, attempted)
				if failures > 1 {
					runs := "runs of failed scans extended"
					if driveWide {
						runs = "drive-wide, runs of failed scans not extended"
					}
					scannerLogAlwaysIf(ctx, fmt.Errorf("data-scanner: cycle %d on %s: %d further bucket scans failed after the first, of %d attempted, %d of them with progress updates forwarded, including %q; %s; last error: %w",
						wantCycle, disk, failures-1, attempted, failuresForwarded, furtherFailed, runs, lastFailure))
				}
				if !driveWide {
					for _, f := range runFailures {
						if n, firstCycle, since, log := elmScanFailed(elmScanKey(er, f.bucket), wantCycle, f.at); log {
							scannerLogAlwaysIf(ctx, fmt.Errorf("data-scanner: %s has failed its scan %d consecutive cycles since cycle %d at %s; root cache %s; last error: %w",
								f.bucket, n, firstCycle, since.UTC().Format(time.RFC3339), f.rootNow, f.err))
						}
					}
				}
				if cancelledDuring != "" {
					elmScannerInfo("data-scanner: cycle %d on %s cancelled (%v) during the scan of %s",
						wantCycle, disk, context.Cause(ctx), cancelledDuring)
				}
			}()

			for bucket := range bucketCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				attempted++ // ELM 2026-10-19, for elmDriveWide.

				// Load cache for bucket
				cacheName := pathJoin(bucket.Name, dataUsageCacheName)
				cache := dataUsageCache{}
				scannerLogIf(ctx, cache.load(ctx, er, cacheName))
				if cache.Info.Name == "" {
					cache.Info.Name = bucket.Name
				}
				cache.Info.SkipHealing = healing
				cache.Info.NextCycle = wantCycle
				if cache.Info.Name != bucket.Name {
					cache.Info = dataUsageCacheInfo{
						Name:       bucket.Name,
						LastUpdate: time.Time{},
						NextCycle:  wantCycle,
					}
				}
				// Collect updates.
				updates := make(chan dataUsageEntry, 1)
				var wg sync.WaitGroup
				// ELM 2026-10-17. Counted for the error log below; read only
				// after wg.Wait.
				var forwarded int
				var lastForwarded dataUsageEntry
				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					for update := range updates {
						select {
						case <-ctx.Done():
						case bucketResults <- dataUsageEntryInfo{
							Name:    name,
							Parent:  dataUsageRoot,
							Entry:   update,
							Partial: true,
						}:
							forwarded++
							lastForwarded = update
						}
					}
				}(cache.Info.Name)
				// Calc usage
				before := cache.Info.LastUpdate
				var err error
				scanStart := time.Now() // ELM 2026-10-17, for the error log below.
				cache, err = disk.NSScanner(ctx, cache, updates, healScanMode, nil)
				scanTook := time.Since(scanStart)
				if err != nil {
					saved := false // ELM 2026-10-17, for the error log below.
					if !cache.Info.LastUpdate.IsZero() && cache.Info.LastUpdate.After(before) {
						saved = true
						scannerLogIf(ctx, cache.save(ctx, er, cacheName))
					} else {
						scannerLogIf(ctx, err)
					}
					// This ensures that we don't close
					// bucketResults channel while the
					// updates-collector goroutine still
					// holds a reference to this.
					wg.Wait()
					// ELM 2026-10-19. The root cache cannot be saved on a
					// done context, so a restore would not persist.
					if ctx.Err() != nil {
						cancelledDuring = bucket.Name
						continue
					}
					// ELM 2026-10-17. On this path the bucket's flattened
					// total is never sent. Any progress update already
					// forwarded is a partial sum that would stand as the
					// bucket's total until its next successful scan, so the
					// bucket's previous entry is sent back in its place. With
					// no previous entry, a new bucket, the partial sum stays.
					// scannerLogIf above drops context.Canceled and "drive
					// not found", which covers grid disconnects, so failures
					// are logged here.
					prev := oldCache.find(bucket.Name)
					var rootNow string
					switch {
					case forwarded == 0:
						rootNow = "keeps the previous entry " + elmUsageFigures(prev)
					case prev == nil:
						rootNow = "keeps the last update, no previous entry"
					default:
						rootNow = "restored to the previous entry " + elmUsageFigures(prev)
					}
					if forwarded > 0 && prev != nil {
						select {
						case <-ctx.Done():
							cancelledDuring = bucket.Name
							continue
						case bucketResults <- dataUsageEntryInfo{
							Name:     bucket.Name,
							Parent:   dataUsageRoot,
							Entry:    *prev,
							Restored: true,
						}:
						}
					}
					failures++
					lastFailure = err
					if forwarded > 0 {
						failuresForwarded++
					}
					if failures > 1 && len(furtherFailed) < 10 {
						furtherFailed = append(furtherFailed, bucket.Name)
					}
					runFailures = append(runFailures, elmPendingScanFailure{bucket: bucket.Name, at: time.Now(), rootNow: rootNow, err: err})
					if failures == 1 {
						cacheSaved := "per-bucket cache not saved"
						if saved {
							cacheSaved = "per-bucket cache saved"
						}
						var last *dataUsageEntry
						if forwarded > 0 {
							last = &lastForwarded
						}
						scannerLogAlwaysIf(ctx, fmt.Errorf("data-scanner: %s cycle %d on %s failed after %s, flattened total not sent, %s; %d updates forwarded, last %s; root cache %s: %w",
							bucket.Name, wantCycle, disk, scanTook.Round(time.Millisecond), cacheSaved, forwarded, elmUsageFigures(last), rootNow, err))
					}
					continue
				}

				wg.Wait()
				// ELM 2026-10-17. Log the end of a run of failed scans long
				// enough to have been logged.
				if n, since := elmScanSucceeded(elmScanKey(er, bucket.Name)); n >= elmScanFailureFirst {
					scannerLogAlwaysIf(ctx, fmt.Errorf("data-scanner: %s scanned successfully in cycle %d after %d consecutive failed cycles over %s",
						bucket.Name, wantCycle, n, time.Since(since).Round(time.Second)))
				}
				// Flatten for upstream, but save full state.
				var root dataUsageEntry
				var topLevel *dataUsageEntry // ELM 2026-10-17, for the saver's log.
				if r := cache.root(); r != nil {
					root = cache.flatten(*r)
					if !r.Compacted {
						topLevel = &dataUsageEntry{Size: r.Size, Objects: r.Objects, Versions: r.Versions}
					}
				}
				select {
				case <-ctx.Done():
					return
				case bucketResults <- dataUsageEntryInfo{
					Name:     cache.Info.Name,
					Parent:   dataUsageRoot,
					Entry:    root,
					TopLevel: topLevel,
				}:
				}

				// Save cache
				scannerLogIf(ctx, cache.save(ctx, er, cacheName))
			}
		}(i)
	}
	wg.Wait()
	xioutil.SafeClose(bucketResults)
	saverWg.Wait()

	return nil
}
