// Copyright (c) 2015-2025 MinIO, Inc.
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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/readahead"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/mimedb"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/minio/sio"
	"github.com/stanford-rc/minio/internal/config/storageclass"
	"github.com/stanford-rc/minio/internal/crypto"
	"github.com/stanford-rc/minio/internal/hash"
	xhttp "github.com/stanford-rc/minio/internal/http"
	xioutil "github.com/stanford-rc/minio/internal/ioutil"
	"github.com/stanford-rc/minio/internal/logger"
)

func (er erasureObjects) getUploadIDDir(bucket, object, uploadID string) string {
	uploadUUID := uploadID
	uploadBytes, err := base64.RawURLEncoding.DecodeString(uploadID)
	if err == nil {
		slc := strings.SplitN(string(uploadBytes), ".", 2)
		if len(slc) == 2 {
			uploadUUID = slc[1]
		}
	}
	return pathJoin(er.getMultipartSHADir(bucket, object), uploadUUID)
}

func (er erasureObjects) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// checkUploadIDExists - verify if a given uploadID exists and is valid.
func (er erasureObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string, write bool) (fi FileInfo, metArr []FileInfo, err error) {
	defer func() {
		if errors.Is(err, errFileNotFound) {
			err = errUploadIDNotFound
		}
	}()

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, bucket, minioMetaMultipartBucket,
		uploadIDPath, "", false, false)

	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, partsMetadata, errs, er.defaultParityCount)
	if err != nil {
		return fi, nil, err
	}

	if readQuorum < 0 {
		return fi, nil, errErasureReadQuorum
	}

	if writeQuorum < 0 {
		return fi, nil, errErasureWriteQuorum
	}

	quorum := readQuorum
	if write {
		quorum = writeQuorum
	}

	// List all online disks.
	_, modTime, etag := listOnlineDisks(storageDisks, partsMetadata, errs, quorum)

	if write {
		err = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	} else {
		err = reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	}
	if err != nil {
		return fi, nil, err
	}

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, etag, quorum)
	return fi, partsMetadata, err
}

// cleanupMultipartPath removes all extraneous files and parts from the multipart folder, this is used per CompleteMultipart.
// do not use this function outside of completeMultipartUpload()
func (er erasureObjects) cleanupMultipartPath(ctx context.Context, paths ...string) {
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].DeleteBulk(ctx, minioMetaMultipartBucket, paths...)
			return nil
		}, index)
	}
	g.Wait()
}

// Clean-up the old multipart uploads. Should be run in a Go routine.
func (er erasureObjects) cleanupStaleUploads(ctx context.Context) {
	// run multiple cleanup's local to this server.
	var wg sync.WaitGroup
	for _, disk := range er.getLocalDisks() {
		if disk != nil {
			wg.Add(1)
			go func(disk StorageAPI) {
				defer wg.Done()
				er.cleanupStaleUploadsOnDisk(ctx, disk)
			}(disk)
		}
	}
	wg.Wait()
}

func (er erasureObjects) deleteAll(ctx context.Context, bucket, prefix string) {
	var wg sync.WaitGroup
	for _, disk := range er.getDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(disk StorageAPI) {
			defer wg.Done()
			disk.Delete(ctx, bucket, prefix, DeleteOptions{
				Recursive: true,
				Immediate: false,
			})
		}(disk)
	}
	wg.Wait()
}

// Remove the old multipart uploads on the given disk.
func (er erasureObjects) cleanupStaleUploadsOnDisk(ctx context.Context, disk StorageAPI) {
	drivePath := disk.Endpoint().Path

	readDirFn(pathJoin(drivePath, minioMetaMultipartBucket), func(shaDir string, typ os.FileMode) error {
		readDirFn(pathJoin(drivePath, minioMetaMultipartBucket, shaDir), func(uploadIDDir string, typ os.FileMode) error {
			uploadIDPath := pathJoin(shaDir, uploadIDDir)
			var modTime time.Time
			// Upload IDs are of the form base64_url(<UUID>x<UnixNano>), we can extract the time from the UUID.
			if b64, err := base64.RawURLEncoding.DecodeString(uploadIDDir); err == nil {
				if split := strings.Split(string(b64), "x"); len(split) == 2 {
					t, err := strconv.ParseInt(split[1], 10, 64)
					if err == nil {
						modTime = time.Unix(0, t)
					}
				}
			}
			// Fallback for older uploads without time in the ID.
			if modTime.IsZero() {
				wait := deleteMultipartCleanupSleeper.Timer(ctx)
				fi, err := disk.ReadVersion(ctx, "", minioMetaMultipartBucket, uploadIDPath, "", ReadOptions{})
				if err != nil {
					return nil
				}
				modTime = fi.ModTime
				wait()
			}
			if time.Since(modTime) < globalAPIConfig.getStaleUploadsExpiry() {
				return nil
			}
			w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
			return w.Run(func() error {
				wait := deleteMultipartCleanupSleeper.Timer(ctx)
				pathUUID := mustGetUUID()
				targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)
				renameAll(pathJoin(drivePath, minioMetaMultipartBucket, uploadIDPath), targetPath, pathJoin(drivePath, minioMetaBucket))
				wait()
				return nil
			})
		})
		// Get the modtime of the shaDir.
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaMultipartBucket, shaDir))
		if err != nil {
			return nil
		}
		// Modtime is returned in the Created field. See (*xlStorage).StatVol
		if time.Since(vi.Created) < globalAPIConfig.getStaleUploadsExpiry() {
			return nil
		}
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		return w.Run(func() error {
			wait := deleteMultipartCleanupSleeper.Timer(ctx)
			pathUUID := mustGetUUID()
			targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)

			// We are not deleting shaDir recursively here, if shaDir is empty
			// and its older then we can happily delete it.
			Rename(pathJoin(drivePath, minioMetaMultipartBucket, shaDir), targetPath)
			wait()
			return nil
		})
	})

	readDirFn(pathJoin(drivePath, minioMetaTmpBucket), func(tmpDir string, typ os.FileMode) error {
		if strings.HasPrefix(tmpDir, ".trash") {
			// do not remove .trash/ here, it has its own routines
			return nil
		}
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaTmpBucket, tmpDir))
		if err != nil {
			return nil
		}
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		return w.Run(func() error {
			wait := deleteMultipartCleanupSleeper.Timer(ctx)
			if time.Since(vi.Created) > globalAPIConfig.getStaleUploadsExpiry() {
				pathUUID := mustGetUUID()
				targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)

				renameAll(pathJoin(drivePath, minioMetaTmpBucket, tmpDir), targetPath, pathJoin(drivePath, minioMetaBucket))
			}
			wait()
			return nil
		})
	})
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
func (er erasureObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	auditObjectErasureSet(ctx, "ListMultipartUploads", object, &er)

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	var uploadIDs []string
	var disk StorageAPI
	disks := er.getOnlineLocalDisks()
	if len(disks) == 0 {
		// If no local, get non-healing disks.
		var ok bool
		if disks, ok = er.getOnlineDisksWithHealing(false); !ok {
			disks = er.getOnlineDisks()
		}
	}

	for _, disk = range disks {
		if disk == nil {
			continue
		}
		if !disk.IsOnline() {
			continue
		}
		uploadIDs, err = disk.ListDir(ctx, bucket, minioMetaMultipartBucket, er.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if errors.Is(err, errDiskNotFound) {
				continue
			}
			if errors.Is(err, errFileNotFound) {
				return result, nil
			}
			return result, toObjectErr(err, bucket, object)
		}
		break
	}

	for i := range uploadIDs {
		uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time, we need
	// to read the metadata entry.
	var uploads []MultipartInfo

	populatedUploadIDs := set.NewStringSet()

	for _, uploadID := range uploadIDs {
		if populatedUploadIDs.Contains(uploadID) {
			continue
		}
		// If present, use time stored in ID.
		startTime := time.Now()
		if split := strings.Split(uploadID, "x"); len(split) == 2 {
			t, err := strconv.ParseInt(split[1], 10, 64)
			if err == nil {
				startTime = time.Unix(0, t)
			}
		}
		uploads = append(uploads, MultipartInfo{
			Bucket:    bucket,
			Object:    object,
			UploadID:  base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%s.%s", globalDeploymentID(), uploadID)),
			Initiated: startTime,
		})
		populatedUploadIDs.Add(uploadID)
	}

	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// newMultipartUpload - wrapper for initializing a new multipart
// request; returns a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at
// '.minio.sys/multipart/bucket/object/uploads.json' on all the
// disks. `uploads.json` carries metadata regarding on-going multipart
// operation(s) on the object.
func (er erasureObjects) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return nil, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			return nil, err
		}

		// if object doesn't exist return error for If-Match conditional requests
		// If-None-Match should be allowed to proceed for non-existent objects
		if err != nil && opts.HasIfMatch && (isErrObjectNotFound(err) || isErrVersionNotFound(err)) {
			return nil, err
		}
	}

	userDefined := cloneMSS(opts.UserDefined)
	if opts.PreserveETag != "" {
		userDefined["etag"] = opts.PreserveETag
	}
	onlineDisks := er.getDisks()

	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(userDefined[xhttp.AmzStorageClass])
	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
	}

	if globalStorageClass.AvailabilityOptimized() {
		// If we have offline disks upgrade the number of erasure codes for this object.
		parityOrig := parityDrives

		var offlineDrives int
		for _, disk := range onlineDisks {
			if disk == nil || !disk.IsOnline() {
				parityDrives++
				offlineDrives++
				continue
			}
		}

		if offlineDrives >= (len(onlineDisks)+1)/2 {
			// if offline drives are more than 50% of the drives
			// we have no quorum, we shouldn't proceed just
			// fail at that point.
			return nil, toObjectErr(errErasureWriteQuorum, bucket, object)
		}

		if parityDrives >= len(onlineDisks)/2 {
			parityDrives = len(onlineDisks) / 2
		}

		if parityOrig != parityDrives {
			userDefined[minIOErasureUpgraded] = strconv.Itoa(parityOrig) + "->" + strconv.Itoa(parityDrives)
		}
	}

	dataDrives := len(onlineDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(onlineDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}
	fi.DataDir = mustGetUUID()

	if ckSum := userDefined[ReplicationSsecChecksumHeader]; ckSum != "" {
		v, err := base64.StdEncoding.DecodeString(ckSum)
		if err == nil {
			fi.Checksum = v
		}
		delete(userDefined, ReplicationSsecChecksumHeader)
	}

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Guess content-type from the extension if possible.
	if userDefined["content-type"] == "" {
		userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	// if storageClass is standard no need to save it as part of metadata.
	if userDefined[xhttp.AmzStorageClass] == storageclass.STANDARD {
		delete(userDefined, xhttp.AmzStorageClass)
	}

	if opts.WantChecksum != nil && opts.WantChecksum.Type.IsSet() {
		userDefined[hash.MinIOMultipartChecksum] = opts.WantChecksum.Type.String()
		userDefined[hash.MinIOMultipartChecksumType] = opts.WantChecksum.Type.ObjType()
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(onlineDisks, partsMetadata, fi)

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Fresh = true
		partsMetadata[index].ModTime = modTime
		partsMetadata[index].Metadata = userDefined
	}
	uploadUUID := fmt.Sprintf("%sx%d", mustGetUUID(), modTime.UnixNano())
	uploadID := base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%s.%s", globalDeploymentID(), uploadUUID))
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadUUID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeAllMetadata(ctx, onlineDisks, bucket, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	return &NewMultipartUploadResult{
		UploadID:     uploadID,
		ChecksumAlgo: userDefined[hash.MinIOMultipartChecksum],
		ChecksumType: userDefined[hash.MinIOMultipartChecksumType],
	}, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (er erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "NewMultipartUpload", object, &er)
	}

	return er.newMultipartUpload(ctx, bucket, object, opts)
}

// renamePart - renames multipart part to its relevant location under uploadID.
func (er erasureObjects) renamePart(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, optsMeta []byte, writeQuorum int, skipParent string) ([]StorageAPI, error) {
	paths := []string{
		dstEntry,
		dstEntry + ".meta",
	}

	// cleanup existing paths first across all drives.
	er.cleanupMultipartPath(ctx, paths...)

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].RenamePart(ctx, srcBucket, srcEntry, dstBucket, dstEntry, optsMeta, skipParent)
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err != nil {
		er.cleanupMultipartPath(ctx, paths...)
	}

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	return evalDisks(disks, errs), err
}

// Write-set enforcement for multipart parts. Used by PutObjectPart at both of its
// narrowing points, and by CompleteMultipartUpload at the commit boundary.
//
// ONE POLICY, NOT A MENU. A part that did not land on every drive it was attempted
// on is ACCEPTED when it is still healable, and the repair is queued at the commit
// boundary. It is REFUSED only when the shortfall leaves it unhealable, because a
// 200 would then be a promise the cluster cannot keep.
//
// MINIO_MULTIPART_WRITESET=off restores the upstream code path. That is a rollback
// lever rather than a policy choice, for getting a running cluster back to known
// behaviour without a rebuild. The unrecoverable case is refused even when off,
// which is what upstream's own write quorum is supposed to do anyway.
func multipartWriteSetEnabled() bool {
	return !strings.EqualFold(env.Get("MINIO_MULTIPART_WRITESET", "on"), "off")
}

// WHY A HEALABLE PART IS ACCEPTED RATHER THAN REFUSED.
//
// Refusing is only useful if the client's re-send is likely to succeed, and that
// depends on whether these failures are independent. They are not. Measured on
// production, 2026-07-31, a rebuild day: 416 shard-write failures against 6,821
// parts, so 6.1% of parts would have been refused. Median inter-arrival 2.0 s, and
// 49% of failures had another failure to the SAME drive within 300 s, about how
// long one 5 GiB part takes. They arrive in bursts.
//
// So a re-sent part often meets the same condition that refused it, and with a
// client retry budget of two or three the upload fails outright. Refusing would
// convert silent degradation into failed multi-terabyte uploads during exactly the
// windows when the cluster is already struggling, and rebuilds last 60 to 67 hours.
// The client mix makes it worse: Globus is 90.48% of PutObjectPart over 90 days,
// and Globus Support ticket #392783 establishes that a single 503 makes it cancel
// and DELETE every in-flight upload in the batch, by design, with no retry.
//
// An earlier revision tried to have both, refusing while refusals were rare and
// degrading to accept-and-heal once they clustered, through a sliding-window
// breaker keyed on a global event ring. REMOVED 2026-09-08. It made the server's
// answer to identical input depend on unrelated concurrent traffic, which is
// untestable in production and the wrong thing to reason about mid-incident, and
// it bought nothing that heal does not now deliver.
//
// What makes accept-and-heal sound is that heal genuinely repairs this shape on the
// tree this patch targets: upstream 16f8cf1c5, "heal: Include more use case of not
// healable but readable objects", is an ancestor of 34fbb95aa. It is NOT in
// RELEASE.2024-08-26, where heal cannot repair a readable-but-incomplete object at
// all, so a queued repair there is a no-op and this patch MUST NOT be back-ported
// to that binary without also taking 16f8cf1c5.

// enforceWriteSet compares the drives a part was ATTEMPTED on against the drives
// that still hold it, and applies the configured policy to any shortfall.
//
// Called at BOTH narrowing points inside PutObjectPart, because they are separate
// faults with the same consequence and the same remedy:
//
//	stage "encode"  erasure.Encode returns success as soon as writeQuorum drives
//	                accept, and multiWriter nils the failed writer and swallows its
//	                error (cmd/erasure-encode.go).
//	stage "rename"  renamePart ends in reduceWriteQuorumErrs + evalDisks, so with
//	                writeQuorum renames succeeding it returns nil and nils the
//	                drive that failed. Worse than the encode case: the errors it
//	                drops are members of objectOpIgnoredErrs, so they are DISCARDED
//	                rather than outvoted, and reduceQuorumErrs logs nothing.
//
// Proven by TestRenamePartShortfallIsSilent: guarding only the encode site leaves a
// 1-of-4 rename failure accepted, identically to an unpatched tree.
//
// Acting here rather than at commit matters because the client still holds the
// data. A refused part costs one re-send and restores FULL redundancy; a part
// accepted and healed later reconstructs to the erasure minimum with no margin.
// There is no server-side retry available: the shard's only copy was the request
// body, already consumed, and storageRESTClient.CreateFile cannot retry a stream it
// has drained.
//
// The HEAL half of this work is Fix A in CompleteMultipartUpload, NOT here. Queueing
// a repair from PutObjectPart is inert: the object does not exist until the commit
// runs, so healRoutine finds nothing one second later, and on an overwrite it heals
// the previous version instead. So this function only ever refuses or lets through.
func (er erasureObjects) enforceWriteSet(ctx context.Context, stage, bucket, object string, partID int,
	attemptedDisks, survivors []StorageAPI, landed, attempted, dataBlocks int,
) error {
	if landed >= attempted {
		return nil
	}

	// Split the shortfall by drive health. A drive that failed and still reports
	// itself online is a transient fault: the client can re-send and get full
	// redundancy back. A drive already known bad is a degraded cluster, and refusing
	// writes on its behalf would take the tier down rather than protect it.
	//
	// Endpoints are named, not just counted. Without the identity a shortfall event
	// cannot be correlated with a rebuild window, a drive-timeout series, or the
	// inter-node offline messages, which is every question we want to ask of it.
	var lostHealthy, lostUnhealthy []string
	for i := range attemptedDisks {
		if attemptedDisks[i] == nil || (i < len(survivors) && survivors[i] != nil) {
			continue
		}
		if attemptedDisks[i].IsOnline() {
			lostHealthy = append(lostHealthy, attemptedDisks[i].String())
		} else {
			lostUnhealthy = append(lostUnhealthy, attemptedDisks[i].String())
		}
	}

	// Unrecoverable is refused whether or not enforcement is on: fewer surviving
	// shards than data blocks cannot be reconstructed from parity, so returning 200
	// would be a promise the cluster cannot keep. Under EC:1 writeQuorum already
	// equals DataBlocks so Encode should have failed first; defence in depth.
	if landed < dataBlocks {
		storageLogIf(ctx, fmt.Errorf(
			"multipart write-set shortfall at %s on %s/%s part %d: landed on %d of %d attempted drives (lost healthy %v, lost unhealthy %v, data blocks %d); UNRECOVERABLE, refusing the part because it could not be reconstructed from parity",
			stage, bucket, object, partID, landed, attempted, lostHealthy, lostUnhealthy, dataBlocks))
		return toObjectErr(errErasureWriteQuorum, bucket, object)
	}

	// Healable, so accepted. The commit boundary detects the resulting divergence
	// and queues the repair there, where the object actually exists. Queueing from
	// here is inert, for the reason given above.
	if multipartWriteSetEnabled() {
		storageLogIf(ctx, fmt.Errorf(
			"multipart write-set shortfall at %s on %s/%s part %d: landed on %d of %d attempted drives (lost healthy %v, lost unhealthy %v); healable, accepted, commit-boundary detection will queue the heal",
			stage, bucket, object, partID, landed, attempted, lostHealthy, lostUnhealthy))
	}

	return nil
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (er erasureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "PutObjectPart", object, &er)
	}

	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// Validates if upload ID exists.
	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return pi, toObjectErr(err, bucket)
		}
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	onlineDisks := er.getDisks()
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())

	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		if r.ContentCRCType().String() != cs {
			return pi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum missing, want %q, got %q", cs, r.ContentCRCType().String()),
			}
		}
	}
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	partSuffix := fmt.Sprintf("part.%d", partID)
	// Random UUID and timestamp for temporary part file.
	tmpPart := fmt.Sprintf("%sx%d", mustGetUUID(), time.Now().UnixNano())
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	defer func() {
		if countOnlineDisks(onlineDisks) != len(onlineDisks) {
			er.deleteAll(context.Background(), minioMetaTmpBucket, tmpPart)
		}
	}()

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate at least a byte to reach EOF
	case size >= fi.Erasure.BlockSize || size == -1:
		if int64(globalBytePoolCap.Load().Width()) < fi.Erasure.BlockSize {
			buffer = make([]byte, fi.Erasure.BlockSize, 2*fi.Erasure.BlockSize)
		} else {
			buffer = globalBytePoolCap.Load().Get()
			defer globalBytePoolCap.Load().Put(buffer)
		}
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully fi.Erasure.BlockSize buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}
	writers := make([]io.Writer, len(onlineDisks))
	// Which drives this part was ATTEMPTED on. A drive already offline is not in
	// this set, so it can never be blamed below: only a drive that accepted the
	// attempt and then failed during the write counts.
	attempted := 0
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		attempted++
		writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}
	// Keep the handles: after Encode the entries in onlineDisks get nil'd, and the
	// health of the drive that failed is what separates a transient fault from a
	// broken drive.
	attemptedDisks := make([]StorageAPI, len(onlineDisks))
	copy(attemptedDisks, onlineDisks)

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// Add input readahead.
		// We use 2 buffers, so we always have a full buffer of input.
		pool := globalBytePoolCap.Load()
		bufA := pool.Get()
		bufB := pool.Get()
		defer pool.Put(bufA)
		defer pool.Put(bufB)
		ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:fi.Erasure.BlockSize], bufB[:fi.Erasure.BlockSize]})
		if err == nil {
			toEncode = ra
			defer ra.Close()
		}
	}

	n, err := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
	closeErrs := closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}
	if closeErr := reduceWriteQuorumErrs(ctx, closeErrs, objectOpIgnoredErrs, writeQuorum); closeErr != nil {
		return pi, toObjectErr(closeErr, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{Bucket: bucket, Object: object}
	}

	// This loop is where the identity of the failed drive has always been known,
	// and where it was always discarded. erasure.Encode returns success as soon as
	// writeQuorum drives accept, and multiWriter nils any writer that failed and
	// swallows its error (cmd/erasure-encode.go). So a part can be admitted on 3 of
	// 4 drives, return 200, and leave a shard that never existed. Nothing
	// downstream compares the set: CompleteMultipartUpload's only self-repair hook
	// asks whether a drive is OFFLINE, and a drive that merely missed one write is
	// still online.
	//
	// Acting here rather than at commit matters because the client is still holding
	// the data. A refused part costs one re-send and restores FULL redundancy; a
	// part accepted and healed later reconstructs to the erasure minimum with no
	// margin. There is no server-side retry available: the shard's only copy was the
	// request body, already consumed, and storageRESTClient.CreateFile cannot retry
	// a stream it has drained.
	landed := 0
	for i := range writers {
		if writers[i] == nil {
			onlineDisks[i] = nil
			continue
		}
		landed++
	}

	if err := er.enforceWriteSet(ctx, "encode", bucket, object, partID,
		attemptedDisks, onlineDisks, landed, attempted, fi.Erasure.DataBlocks); err != nil {
		return pi, err
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)

	md5hex := r.MD5CurrentHexString()
	if opts.PreserveETag != "" {
		md5hex = opts.PreserveETag
	}

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	actualSize := data.ActualSize()
	if actualSize < 0 {
		_, encrypted := crypto.IsEncrypted(fi.Metadata)
		compressed := fi.IsCompressed()
		switch {
		case compressed:
			// ... nothing changes for compressed stream.
			// if actualSize is -1 we have no known way to
			// determine what is the actualSize.
		case encrypted:
			decSize, err := sio.DecryptedSize(uint64(n))
			if err == nil {
				actualSize = int64(decSize)
			}
		default:
			actualSize = n
		}
	}

	partInfo := ObjectPartInfo{
		Number:     partID,
		ETag:       md5hex,
		Size:       n,
		ActualSize: actualSize,
		ModTime:    UTCNow(),
		Index:      index,
		Checksums:  r.ContentCRC(),
	}

	partFI, err := partInfo.MarshalMsg(nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Serialize concurrent part uploads.
	partIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID, strconv.Itoa(partID)))
	plkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}

	ctx = plkctx.Context()
	defer partIDLock.Unlock(plkctx)

	// Read lock for upload id, only held while reading the upload metadata.
	uploadIDRLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	rlkctx, err := uploadIDRLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}
	ctx = rlkctx.Context()
	defer uploadIDRLock.RUnlock(rlkctx)

	// The drives that still held the shard going into the rename. renamePart nils
	// the ones whose rename failed, so this is the only surviving record of what was
	// attempted at this stage.
	renameAttempted := make([]StorageAPI, len(onlineDisks))
	copy(renameAttempted, onlineDisks)
	renameAttemptedCount := 0
	for i := range renameAttempted {
		if renameAttempted[i] != nil {
			renameAttemptedCount++
		}
	}

	onlineDisks, err = er.renamePart(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, partFI, writeQuorum, uploadIDPath)
	if err != nil {
		if errors.Is(err, errUploadIDNotFound) {
			return pi, toObjectErr(errUploadIDNotFound, bucket, object, uploadID)
		}
		if errors.Is(err, errFileNotFound) {
			// An in-quorum errFileNotFound means that client stream
			// prematurely closed and we do not find any xl.meta or
			// part.1's - in such a scenario we must return as if client
			// disconnected. This means that erasure.Encode() CreateFile()
			// did not do anything.
			return pi, IncompleteBody{Bucket: bucket, Object: object}
		}

		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// The second narrowing, and the one A-prime originally missed. renamePart
	// returned nil because writeQuorum renames succeeded, while evalDisks quietly
	// nil'd any drive whose rename did not.
	renameLanded := 0
	for i := range onlineDisks {
		if onlineDisks[i] != nil {
			renameLanded++
		}
	}
	if err := er.enforceWriteSet(ctx, "rename", bucket, object, partID,
		renameAttempted, onlineDisks, renameLanded, renameAttemptedCount, fi.Erasure.DataBlocks); err != nil {
		return pi, err
	}

	// Return success.
	return PartInfo{
		PartNumber:        partInfo.Number,
		ETag:              partInfo.ETag,
		LastModified:      partInfo.ModTime,
		Size:              partInfo.Size,
		ActualSize:        partInfo.ActualSize,
		ChecksumCRC32:     partInfo.Checksums["CRC32"],
		ChecksumCRC32C:    partInfo.Checksums["CRC32C"],
		ChecksumSHA1:      partInfo.Checksums["SHA1"],
		ChecksumSHA256:    partInfo.Checksums["SHA256"],
		ChecksumCRC64NVME: partInfo.Checksums["CRC64NVME"],
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
// Does not contain currently uploaded parts by design.
func (er erasureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "GetMultipartInfo", object, &er)
	}

	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return result, toObjectErr(err, bucket)
		}
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	result.UserDefined = cloneMSS(fi.Metadata)
	return result, nil
}

func (er erasureObjects) listParts(ctx context.Context, onlineDisks []StorageAPI, partPath string, readQuorum int) ([]int, error) {
	g := errgroup.WithNErrs(len(onlineDisks))

	objectParts := make([][]string, len(onlineDisks))
	// List uploaded parts from drives.
	for index := range onlineDisks {
		g.Go(func() (err error) {
			if onlineDisks[index] == nil {
				return errDiskNotFound
			}
			objectParts[index], err = onlineDisks[index].ListDir(ctx, minioMetaMultipartBucket, minioMetaMultipartBucket, partPath, -1)
			return err
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum); err != nil {
		return nil, err
	}

	partQuorumMap := make(map[int]int)
	for _, driveParts := range objectParts {
		partsWithMetaCount := make(map[int]int, len(driveParts))
		// part files can be either part.N or part.N.meta
		for _, partPath := range driveParts {
			var partNum int
			if _, err := fmt.Sscanf(partPath, "part.%d", &partNum); err == nil {
				partsWithMetaCount[partNum]++
				continue
			}
			if _, err := fmt.Sscanf(partPath, "part.%d.meta", &partNum); err == nil {
				partsWithMetaCount[partNum]++
			}
		}
		// Include only part.N.meta files with corresponding part.N
		for partNum, cnt := range partsWithMetaCount {
			if cnt < 2 {
				continue
			}
			partQuorumMap[partNum]++
		}
	}

	var partNums []int
	for partNum, count := range partQuorumMap {
		if count < readQuorum {
			continue
		}
		partNums = append(partNums, partNum)
	}

	sort.Ints(partNums)
	return partNums, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (er erasureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "ListObjectParts", object, &er)
	}

	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	if partNumberMarker < 0 {
		partNumberMarker = 0
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = cloneMSS(fi.Metadata)
	result.ChecksumAlgorithm = fi.Metadata[hash.MinIOMultipartChecksum]
	result.ChecksumType = fi.Metadata[hash.MinIOMultipartChecksumType]

	if maxParts == 0 {
		return result, nil
	}

	onlineDisks := er.getDisks()
	readQuorum := fi.ReadQuorum(er.defaultRQuorum())
	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + SlashSeparator

	// List parts in quorum
	partNums, err := er.listParts(ctx, onlineDisks, partPath, readQuorum)
	if err != nil {
		// This means that fi.DataDir, is not yet populated so we
		// return an empty response.
		if errors.Is(err, errFileNotFound) {
			return result, nil
		}
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	if len(partNums) == 0 {
		return result, nil
	}

	start := objectPartIndexNums(partNums, partNumberMarker)
	if partNumberMarker > 0 && start == -1 {
		// Marker not present among what is present on the
		// server, we return an empty list.
		return result, nil
	}

	if partNumberMarker > 0 && start != -1 {
		if start+1 >= len(partNums) {
			// Marker indicates that we are the end
			// of the list, so we simply return empty
			return result, nil
		}

		partNums = partNums[start+1:]
	}

	result.Parts = make([]PartInfo, 0, len(partNums))
	partMetaPaths := make([]string, len(partNums))
	for i, part := range partNums {
		partMetaPaths[i] = pathJoin(partPath, fmt.Sprintf("part.%d.meta", part))
	}

	// Read parts in quorum. The divergence report is discarded here: this is a
	// read-only listing and queueing a heal from it would let any client trigger
	// repair work. CompleteMultipartUpload is where it is acted on.
	objParts, _, _, err := readParts(ctx, onlineDisks, minioMetaMultipartBucket, partMetaPaths,
		partNums, readQuorum)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	count := maxParts
	for _, objPart := range objParts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:        objPart.Number,
			LastModified:      objPart.ModTime,
			ETag:              objPart.ETag,
			Size:              objPart.Size,
			ActualSize:        objPart.ActualSize,
			ChecksumCRC32:     objPart.Checksums["CRC32"],
			ChecksumCRC32C:    objPart.Checksums["CRC32C"],
			ChecksumSHA1:      objPart.Checksums["SHA1"],
			ChecksumSHA256:    objPart.Checksums["SHA256"],
			ChecksumCRC64NVME: objPart.Checksums["CRC64NVME"],
		})
		count--
		if count == 0 {
			break
		}
	}

	if len(objParts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is true for subsequent listing.
		result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
	}

	return result, nil
}

// partPlacement is what readParts learned about which drives hold which parts.
//
// KEYED BY THE DRIVE'S WITHIN-SET INDEX from StorageAPI.GetDiskLoc(), never by
// position in the disks slice. CompleteMultipartUpload calls
// shuffleDisksAndPartsMetadataByIndex between readParts and renameData, so a
// position-keyed mask would silently misalign against the post-rename drive set and
// the commit check below would compare the wrong drives. GetDiskLoc returns
// endpoint.DiskIdx, assigned once per erasure set at startup
// (cmd/endpoint.go:1005), so it is stable for the life of the process.
type partPlacement struct {
	held   []uint64 // per part: bitmask of drive indices holding a usable part.N.meta
	usable uint64   // drives usable at readParts time
	valid  bool     // false if the set is wider than a uint64, in which case skip
}

// unreadableAfter reports the parts that fall below dataBlocks once the drive set is
// narrowed to `committed`. That is the difference between a part that merely lost
// redundancy and one that can no longer be reconstructed at all.
func (p partPlacement) unreadableAfter(committed uint64, dataBlocks int) []int {
	if !p.valid {
		return nil
	}
	var out []int
	for pidx, m := range p.held {
		if m == 0 {
			continue // no drive served it; already reported through partInfosInQuorum
		}
		if bits.OnesCount64(m&committed) < dataBlocks {
			out = append(out, pidx)
		}
	}
	return out
}

// healableDivergence compares OUR healable verdict against MINIO'S OWN and reports
// the parts where the two disagree.
//
// WHY BOTH ARE COMPUTED. MinIO decides a part is fine by counting ETag votes across
// every drive it read, in readParts: `partMetaQuorumMap[maxETag] >= readQuorum`,
// which leaves `partInfosInQuorum[pidx].Error` empty. That count is not scoped to
// the drives that actually committed the object, and in RELEASE.2024-08-26 the vote
// map is built outside the per-drive loop, so a part present on ONE of four drives
// can satisfy it. Acting on that verdict is how a 200 gets returned for something
// that cannot be read back.
//
// Ours is the placement bitmask intersected with the committed set, which is the
// question that actually matters: of the drives that hold this part, how many are in
// the set the object was finalized against. That is what unreadableAfter answers and
// it is what this patch acts on.
//
// The two should agree. Where they do not, the disagreement is evidence about a real
// defect in production rather than a hypothetical, so it is logged loudly and left
// for triage. We do NOT act on MinIO's verdict, in either direction.
//
// Returns the part indices where MinIO says the part is fine and we say it is below
// dataBlocks among committed drives ("optimistic"), and the reverse ("pessimistic").
func healableDivergence(placement partPlacement, partInfos []ObjectPartInfo,
	committed uint64, dataBlocks int,
) (optimistic, pessimistic []int) {
	if !placement.valid {
		return nil, nil
	}
	for pidx, m := range placement.held {
		if m == 0 {
			continue // no drive served it; reported through partInfosInQuorum already
		}
		if pidx >= len(partInfos) {
			continue
		}
		theirsOK := partInfos[pidx].Error == ""
		oursOK := bits.OnesCount64(m&committed) >= dataBlocks
		switch {
		case theirsOK && !oursOK:
			optimistic = append(optimistic, pidx)
		case !theirsOK && oursOK:
			pessimistic = append(pessimistic, pidx)
		}
	}
	return optimistic, pessimistic
}

// shouldQueueWriteSetHeal reports whether a write-set shortfall should be queued
// for heal.
//
// RECOMMENDATION 3, 2026-08-31.  The below-quorum case is ALREADY unreachable here,
// and this exists because it is unreachable only emergently.  Two facts combine:
//
//   - readParts gates `underReplicated` and `partPlacement.valid` on the same
//     trackSets flag, so when the drive set is too wide to track, no shortfall is
//     reported and nothing is queued.
//   - the commit-set collapse check runs earlier in CompleteMultipartUpload and
//     RETURNS InvalidPart when any part falls below dataBlocks, so control never
//     reaches the queue in that state.
//
// Neither fact is local to the queueing site, and nothing there says so.  A future
// edit that reorders those blocks, or adds a return path around the collapse check,
// would silently reopen the hole with no comment to warn against it.  Stating the
// condition here makes it enforced locally rather than inferred from two other
// places.
//
// WHY IT MATTERS.  Queueing a heal for a part below read quorum is worse than
// useless.  Heal cannot repair it: master's healObject sets cannotHeal on exactly
// that condition.  What it does instead is call deleteIfDangling, which removes the
// whole object VERSION from every drive, so one part below quorum in a 203-part
// object destroys all 203 including the ~200 intact ones.  That path is gated on
// neither opts.Remove nor a scan mode.  An object in that state needs a person, not
// a heal.
func shouldQueueWriteSetHeal(placement partPlacement, underReplicated []int,
	committed uint64, dataBlocks int,
) (bool, []int) {
	if len(underReplicated) == 0 {
		return false, nil
	}
	// Below read quorum: not a redundancy problem, a loss. Report the parts so the
	// caller can name them rather than logging an unexplained refusal.
	if lost := placement.unreadableAfter(committed, dataBlocks); len(lost) > 0 {
		return false, lost
	}
	return true, nil
}

// readParts additionally reports which parts are held on a strictly smaller set
// of drives than the object's usable write set. That divergence is the silent
// multipart loss: a part admitted at one write-quorum subset while the object is
// finalized against another, with nothing comparing the two.
//
// FIX A. This is the commit-boundary half of the write-set work, and it is the
// necessary complement to the PutObjectPart check rather than a duplicate of it.
// The part-boundary check can only see erasure.Encode; it runs BEFORE renamePart
// and is structurally blind to every narrowing downstream of itself. Proven by
// TestRenamePartShortfallIsSilent, where a 1-of-4 RenamePart failure is accepted
// identically with and without that check. This one keys on what actually landed,
// so it catches renamePart, a failed .meta write, and readParts' own count-based
// accept, whatever their cause.
//
// Everything needed is already read here. objectPartInfos[disk][part] is a full
// 2-D presence matrix, and the loop below collapses it to a per-part COUNT, which
// discards drive identity. Each pidx iteration is independent, so no part is ever
// compared against another. One bitmask per part closes that, with no metadata
// change and no extra I/O: O(drives x parts) integer ops on data already in memory.
//
// A drive down for the whole upload is missing from EVERY part's set, so the sets
// agree and nothing is reported. Only a drive that took some parts and not others
// produces a strict subset, which is exactly the fault.
func readParts(ctx context.Context, disks []StorageAPI, bucket string, partMetaPaths []string, partNumbers []int, readQuorum int) ([]ObjectPartInfo, []int, partPlacement, error) {
	g := errgroup.WithNErrs(len(disks))

	objectPartInfos := make([][]*ObjectPartInfo, len(disks))
	// Rename file on all underlying storage disks.
	for index := range disks {
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			objectPartInfos[index], err = disks[index].ReadParts(ctx, bucket, partMetaPaths...)
			return err
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum); err != nil {
		return nil, nil, partPlacement{}, err
	}

	// One bitmask per part: which drives hold a usable part.N.meta. Bounded to 64
	// drives, far above any erasure set, and the check is SKIPPED rather than
	// wrong if that is ever exceeded.
	// diskBit maps a drive to its bit position using its within-set index rather
	// than its slice position, for the reason given on partPlacement.
	diskBit := make([]int, len(disks))
	trackSets := len(disks) <= 64
	for idx := range disks {
		diskBit[idx] = -1
		if disks[idx] == nil {
			continue
		}
		_, _, d := disks[idx].GetDiskLoc()
		if d < 0 || d >= 64 {
			trackSets = false // outside bitmask width; skip rather than be wrong
			continue
		}
		diskBit[idx] = d
	}
	presence := make([]uint64, len(partMetaPaths))

	// The reference set is the drives USABLE for this read, not the widest set
	// among the parts. Comparing parts only against each other misses a drive that
	// dropped EVERY part, which is equally silent and equally fatal to redundancy.
	// Measured: one drive holding none of an object's parts produced no divergence
	// between siblings and went unreported.
	//
	// A genuinely offline drive is nil here, which is exactly how MinIO represents
	// it, so it is excluded and cannot raise a false positive. A drive that is
	// online but could not serve metadata IS included, and flagging it is correct:
	// heal is the right response to an online drive that cannot answer.
	var usable uint64
	if trackSets {
		for idx := range disks {
			if diskBit[idx] >= 0 {
				usable |= 1 << uint(diskBit[idx])
			}
		}
	}

	partInfosInQuorum := make([]ObjectPartInfo, len(partMetaPaths))
	for pidx := range partMetaPaths {
		// partMetaQuorumMap uses
		//  - path/to/part.N as key to collate errors from failed drives.
		//  - part ETag to collate part metadata
		partMetaQuorumMap := make(map[string]int, len(partNumbers))
		var pinfos []*ObjectPartInfo
		for idx := range disks {
			if len(objectPartInfos[idx]) != len(partMetaPaths) {
				partMetaQuorumMap[partMetaPaths[pidx]]++
				continue
			}

			pinfo := objectPartInfos[idx][pidx]
			if pinfo != nil && pinfo.ETag != "" {
				pinfos = append(pinfos, pinfo)
				partMetaQuorumMap[pinfo.ETag]++
				if trackSets && diskBit[idx] >= 0 {
					presence[pidx] |= 1 << uint(diskBit[idx])
				}
				continue
			}
			partMetaQuorumMap[partMetaPaths[pidx]]++
		}

		var maxQuorum int
		var maxETag string
		var maxPartMeta string
		for etag, quorum := range partMetaQuorumMap {
			if maxQuorum < quorum {
				maxQuorum = quorum
				maxETag = etag
				maxPartMeta = etag
			}
		}
		// found is a representative ObjectPartInfo which either has the maximally occurring ETag or an error.
		var found *ObjectPartInfo
		for _, pinfo := range pinfos {
			if pinfo == nil {
				continue
			}
			if maxETag != "" && pinfo.ETag == maxETag {
				found = pinfo
				break
			}
			if pinfo.ETag == "" && maxPartMeta != "" && path.Base(maxPartMeta) == fmt.Sprintf("part.%d.meta", pinfo.Number) {
				found = pinfo
				break
			}
		}

		if found != nil && found.ETag != "" && partMetaQuorumMap[maxETag] >= readQuorum {
			partInfosInQuorum[pidx] = *found
			continue
		}
		partInfosInQuorum[pidx] = ObjectPartInfo{
			Number: partNumbers[pidx],
			Error: InvalidPart{
				PartNumber: partNumbers[pidx],
			}.Error(),
		}
	}

	// Compare the sets. Any part held on a strict subset of the usable drives was
	// admitted at fewer drives than the object was finalized against.
	var underReplicated []int
	if trackSets {
		for pidx, m := range presence {
			// Only parts that are otherwise fine: a part already carrying an error
			// is reported through partInfosInQuorum and needs no second channel.
			// m == 0 means no drive served it at all, which is the same case.
			if m != usable && m != 0 && partInfosInQuorum[pidx].Error == "" {
				underReplicated = append(underReplicated, pidx)
			}
		}
	}

	return partInfosInQuorum, underReplicated, partPlacement{
		held:   presence,
		usable: usable,
		valid:  trackSets,
	}, nil
}

func objPartToPartErr(part ObjectPartInfo) error {
	if strings.Contains(part.Error, "file not found") {
		return InvalidPart{PartNumber: part.Number}
	}
	if strings.Contains(part.Error, "Specified part could not be found") {
		return InvalidPart{PartNumber: part.Number}
	}
	if strings.Contains(part.Error, errErasureReadQuorum.Error()) {
		return errErasureReadQuorum
	}
	return errors.New(part.Error)
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (er erasureObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "CompleteMultipartUpload", object, &er)
	}

	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return ObjectInfo{}, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return ObjectInfo{}, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) {
			return ObjectInfo{}, err
		}

		// if object doesn't exist return error for If-Match conditional requests
		// If-None-Match should be allowed to proceed for non-existent objects
		if err != nil && opts.HasIfMatch && (isErrObjectNotFound(err) || isErrVersionNotFound(err)) {
			return ObjectInfo{}, err
		}
	}

	fi, partsMetadata, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return oi, toObjectErr(err, bucket)
		}
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	onlineDisks := er.getDisks()
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())
	readQuorum := fi.ReadQuorum(er.defaultRQuorum())

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + SlashSeparator
	partMetaPaths := make([]string, len(parts))
	partNumbers := make([]int, len(parts))
	for idx, part := range parts {
		partMetaPaths[idx] = pathJoin(partPath, fmt.Sprintf("part.%d.meta", part.PartNumber))
		partNumbers[idx] = part.PartNumber
	}

	partInfoFiles, underReplicatedParts, placement, err := readParts(ctx, onlineDisks, minioMetaMultipartBucket, partMetaPaths, partNumbers, readQuorum)
	if err != nil {
		return oi, err
	}

	if len(partInfoFiles) != len(parts) {
		// Should only happen through internal error
		err := fmt.Errorf("unexpected part result count: %d, want %d", len(partInfoFiles), len(parts))
		bugLogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	// Checksum type set when upload started.
	var checksumType hash.ChecksumType
	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		checksumType = hash.NewChecksumType(cs, fi.Metadata[hash.MinIOMultipartChecksumType])
		if opts.WantChecksum != nil && !opts.WantChecksum.Type.Is(checksumType) {
			return oi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum type mismatch. got %q (%s) expected %q (%s)", checksumType.String(), checksumType.ObjType(), opts.WantChecksum.Type.String(), opts.WantChecksum.Type.ObjType()),
			}
		}
		checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
	}

	var checksumCombined []byte

	// However, in case of encryption, the persisted part ETags don't match
	// what we have sent to the client during PutObjectPart. The reason is
	// that ETags are encrypted. Hence, the client will send a list of complete
	// part ETags of which may not match the ETag of any part. For example
	//   ETag (client):          30902184f4e62dd8f98f0aaff810c626
	//   ETag (server-internal): 20000f00ce5dc16e3f3b124f586ae1d88e9caa1c598415c2759bbb50e84a59f630902184f4e62dd8f98f0aaff810c626
	//
	// Therefore, we adjust all ETags sent by the client to match what is stored
	// on the backend.
	kind, _ := crypto.IsEncrypted(fi.Metadata)

	var objectEncryptionKey []byte
	switch kind {
	case crypto.SSEC:
		if checksumType.IsSet() {
			if opts.EncryptFn == nil {
				return oi, crypto.ErrMissingCustomerKey
			}
			baseKey := opts.EncryptFn("", nil)
			if len(baseKey) != 32 {
				return oi, crypto.ErrInvalidCustomerKey
			}
			objectEncryptionKey, err = decryptObjectMeta(baseKey, bucket, object, fi.Metadata)
			if err != nil {
				return oi, err
			}
		}
	case crypto.S3, crypto.S3KMS:
		objectEncryptionKey, err = decryptObjectMeta(nil, bucket, object, fi.Metadata)
		if err != nil {
			return oi, err
		}
	}
	if len(objectEncryptionKey) == 32 {
		var key crypto.ObjectKey
		copy(key[:], objectEncryptionKey)
		opts.EncryptFn = metadataEncrypter(key)
	}

	for idx, part := range partInfoFiles {
		if part.Error != "" {
			err = objPartToPartErr(part)
			bugLogIf(ctx, err)
			return oi, err
		}

		if parts[idx].PartNumber != part.Number {
			internalLogIf(ctx, fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", parts[idx].PartNumber, parts[idx].PartNumber, part.Number))
			return oi, InvalidPart{
				PartNumber: part.Number,
			}
		}

		// Add the current part.
		fi.AddObjectPart(part.Number, part.ETag, part.Size, part.ActualSize, part.ModTime, part.Index, part.Checksums)
	}

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Order online disks in accordance with distribution order.
	// Order parts metadata in accordance with distribution order.
	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadataByIndex(onlineDisks, partsMetadata, fi)

	// Save current erasure metadata for validation.
	currentFI := fi

	// Allocate parts similar to incoming slice.
	fi.Parts = make([]ObjectPartInfo, len(parts))

	var checksum hash.Checksum
	checksum.Type = checksumType

	// Validate each part and then commit to disk.
	for i, part := range parts {
		partIdx := objectPartIndex(currentFI.Parts, part.PartNumber)
		// All parts should have same part number.
		if partIdx == -1 {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
			return oi, invp
		}
		expPart := currentFI.Parts[partIdx]

		// ensure that part ETag is canonicalized to strip off extraneous quotes
		part.ETag = canonicalizeETag(part.ETag)
		expETag := tryDecryptETag(objectEncryptionKey, expPart.ETag, kind == crypto.S3)
		if expETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    expETag,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		if checksumType.IsSet() {
			crc := expPart.Checksums[checksumType.String()]
			if crc == "" {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
				}
			}
			wantCS := map[string]string{
				hash.ChecksumCRC32.String():     part.ChecksumCRC32,
				hash.ChecksumCRC32C.String():    part.ChecksumCRC32C,
				hash.ChecksumSHA1.String():      part.ChecksumSHA1,
				hash.ChecksumSHA256.String():    part.ChecksumSHA256,
				hash.ChecksumCRC64NVME.String(): part.ChecksumCRC64NVME,
			}
			if wantCS[checksumType.String()] != crc {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
					ExpETag:    wantCS[checksumType.String()],
					GotETag:    crc,
				}
			}
			cs := hash.NewChecksumString(checksumType.String(), crc)
			if !cs.Valid() {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
				}
			}
			if checksumType.FullObjectRequested() {
				if err := checksum.AddPart(*cs, expPart.ActualSize); err != nil {
					return oi, InvalidPart{
						PartNumber: part.PartNumber,
						ExpETag:    "<nil>",
						GotETag:    err.Error(),
					}
				}
			}
			checksumCombined = append(checksumCombined, cs.Raw...)
		}

		// All parts except the last part has to be at least 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   expPart.ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += expPart.Size

		// Save the consolidated actual size.
		objectActualSize += expPart.ActualSize

		// Add incoming parts.
		fi.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			Size:       expPart.Size,
			ActualSize: expPart.ActualSize,
			ModTime:    expPart.ModTime,
			Index:      expPart.Index,
			Checksums:  nil, // Not transferred since we do not need it.
		}
	}

	if opts.WantChecksum != nil {
		if checksumType.FullObjectRequested() {
			if opts.WantChecksum.Encoded != checksum.Encoded {
				err := hash.ChecksumMismatch{
					Want: opts.WantChecksum.Encoded,
					Got:  checksum.Encoded,
				}
				return oi, err
			}
		} else {
			err := opts.WantChecksum.Matches(checksumCombined, len(parts))
			if err != nil {
				return oi, err
			}
		}
	}

	// Accept encrypted checksum from incoming request.
	if opts.UserDefined[ReplicationSsecChecksumHeader] != "" {
		if v, err := base64.StdEncoding.DecodeString(opts.UserDefined[ReplicationSsecChecksumHeader]); err == nil {
			fi.Checksum = v
		}
		delete(opts.UserDefined, ReplicationSsecChecksumHeader)
	}

	if checksumType.IsSet() {
		checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
		checksum.Type = checksumType
		if !checksumType.FullObjectRequested() {
			checksum = *hash.NewChecksumFromData(checksumType, checksumCombined)
		}
		fi.Checksum = checksum.AppendTo(nil, checksumCombined)
		if opts.EncryptFn != nil {
			fi.Checksum = opts.EncryptFn("object-checksum", fi.Checksum)
		}
	}
	// Remove superfluous internal headers.
	delete(fi.Metadata, hash.MinIOMultipartChecksum)
	delete(fi.Metadata, hash.MinIOMultipartChecksumType)

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = opts.MTime
	if opts.MTime.IsZero() {
		fi.ModTime = UTCNow()
	}

	// Save successfully calculated md5sum.
	// for replica, newMultipartUpload would have already sent the replication ETag
	if fi.Metadata["etag"] == "" {
		if opts.UserDefined["etag"] != "" {
			fi.Metadata["etag"] = opts.UserDefined["etag"]
		} else { // fallback if not already calculated in handler.
			fi.Metadata["etag"] = getCompleteMultipartMD5(parts)
		}
	}

	// Save the consolidated actual size.
	if opts.ReplicationRequest {
		if v := opts.UserDefined[ReservedMetadataPrefix+"Actual-Object-Size"]; v != "" {
			fi.Metadata[ReservedMetadataPrefix+"actual-size"] = v
		}
	} else {
		fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	}

	if opts.DataMovement {
		fi.SetDataMov()
	}

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		if partsMetadata[index].IsValid() {
			partsMetadata[index].Size = fi.Size
			partsMetadata[index].ModTime = fi.ModTime
			partsMetadata[index].Metadata = fi.Metadata
			partsMetadata[index].Parts = fi.Parts
			partsMetadata[index].Checksum = fi.Checksum
			partsMetadata[index].Versioned = opts.Versioned || opts.VersionSuspended
		}
	}

	paths := make([]string, 0, len(currentFI.Parts))
	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentFI.Parts {
		paths = append(paths, pathJoin(uploadIDPath, currentFI.DataDir, fmt.Sprintf("part.%d.meta", curpart.Number)))

		if objectPartIndex(fi.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			paths = append(paths, pathJoin(uploadIDPath, currentFI.DataDir, fmt.Sprintf("part.%d", curpart.Number)))
		}
	}

	if !opts.NoLock {
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	er.cleanupMultipartPath(ctx, paths...) // cleanup all part.N.meta, and skipped part.N's before final rename().

	defer func() {
		if err == nil {
			er.deleteAll(context.Background(), minioMetaMultipartBucket, uploadIDPath)
		}
	}()

	// Rename the multipart object to final location.
	onlineDisks, versions, oldDataDir, err := renameData(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath,
		partsMetadata, bucket, object, writeQuorum)
	if err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	if err = er.commitRenameDataDir(ctx, bucket, object, oldDataDir, onlineDisks, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object, uploadID)
	}

	// FAIL THE COMMIT when a part has become unreadable.
	//
	// This is the third response, alongside refusing at the part boundary and
	// accepting for repair. It exists because neither of those covers the shape that
	// produced Elm's one confirmed permanent loss: a part admitted at exactly
	// readQuorum, and then renameData committing on writeQuorum drives while
	// EXCLUDING one of the drives that part depended on. The part drops below
	// dataBlocks, no reconstruction is possible, and the pre-existing code path
	// returns 200.
	//
	// readParts already recorded which drives hold each part. renameData has just
	// reported which drives committed, as the non-nil entries of onlineDisks. The
	// intersection is the set that both holds the part and committed it, and if that
	// is smaller than dataBlocks the part cannot be read back. Queueing a repair for
	// it is pointless: there is nothing left to reconstruct from.
	//
	// Failing here also preserves the evidence. The staging directory is removed by a
	// deferred call that is already conditional on `err == nil`, so returning an error
	// leaves the surviving shards in place instead of deleting them. In the traced
	// loss those staging copies were the only recoverable copies and were destroyed
	// 4m39s after the commit.
	//
	// Rate matters and is the reason this is safe where a part-boundary refusal is
	// not. This fires only when data has genuinely been lost, not on every shortfall,
	// so it does not carry the client amplification cost that makes `reject`
	// unusable against a client mix dominated by one that cancels whole transfer runs
	// on any failure.
	if !opts.Speedtest {
		var committed uint64
		for i := range onlineDisks {
			if onlineDisks[i] == nil {
				continue
			}
			if _, _, d := onlineDisks[i].GetDiskLoc(); d >= 0 && d < 64 {
				committed |= 1 << uint(d)
			}
		}
		if lost := placement.unreadableAfter(committed, fi.Erasure.DataBlocks); len(lost) > 0 {
			nums := make([]int, 0, len(lost))
			for _, pidx := range lost {
				if pidx < len(partNumbers) {
					nums = append(nums, partNumbers[pidx])
				}
			}
			storageLogIf(ctx, fmt.Errorf(
				"multipart commit-set collapse on %s/%s: part(s) %v fell below %d data blocks once the commit set narrowed (held %#x, committed %#x, usable at read %#x); failing the commit so the staging shards are retained and the client is told",
				bucket, object, nums, fi.Erasure.DataBlocks, placement.held, committed, placement.usable))
			if len(nums) > 0 {
				return oi, toObjectErr(InvalidPart{PartNumber: nums[0]}, bucket, object, uploadID)
			}
			return oi, toObjectErr(errErasureWriteQuorum, bucket, object, uploadID)
		}
	}

	if !opts.Speedtest && len(versions) > 0 {
		globalMRFState.addPartialOp(PartialOperation{
			Bucket:    bucket,
			Object:    object,
			Queued:    time.Now(),
			Versions:  versions,
			SetIndex:  er.setIndex,
			PoolIndex: er.poolIndex,
		})
	}

	if !opts.Speedtest && len(versions) == 0 {
		// Check if there is any offline disk and add it to the MRF list
		for _, disk := range onlineDisks {
			if disk != nil && disk.IsOnline() {
				continue
			}
			er.addPartial(bucket, object, fi.VersionID)
			break
		}
	}

	// FIX A. The loop above is multipart's only pre-existing self-repair hook, and
	// it fires on `disk == nil || !disk.IsOnline()`. That covers a renameData
	// failure, because evalDisks nils the drive that failed. It CANNOT fire for a
	// part-level shortfall: a drive that merely missed one write mid-upload is
	// still online and still non-nil here. The part is gone and nothing is queued,
	// so the object sits at reduced redundancy until something reads it, which on
	// archive data can be years.
	//
	// Placed AFTER renameData and commitRenameDataDir deliberately. This is the
	// first point at which bucket/object names a real object, so the MRF entry has
	// a target. Queueing the same repair from PutObjectPart does nothing at all:
	// the object does not exist until this function runs, healRoutine waits one
	// second and finds nothing, and on an overwrite it heals the previous version
	// instead. That is why the reject half belongs at the part boundary and the
	// heal half belongs here.
	//
	// This does NOT fail the commit, deliberately. The object is complete and every
	// surviving shard is correct, so it reads back byte-for-byte from the remaining
	// quorum; failing here would make a client re-upload terabytes of good data.
	// What is missing is redundancy, and redundancy is what heal restores.
	if !opts.Speedtest && multipartWriteSetEnabled() {
		// Recompute the commit set rather than reuse the collapse check's local:
		// the predicate has to be evaluated against the same drives here, and
		// borrowing a variable from a block 60 lines up is how the invariant
		// became implicit in the first place.
		var committed uint64
		for i := range onlineDisks {
			if onlineDisks[i] == nil {
				continue
			}
			if _, _, d := onlineDisks[i].GetDiskLoc(); d >= 0 && d < 64 {
				committed |= 1 << uint(d)
			}
		}

		queue, lost := shouldQueueWriteSetHeal(placement, underReplicatedParts,
			committed, fi.Erasure.DataBlocks)

		partNums := func(idxs []int) []int {
			out := make([]int, 0, len(idxs))
			for _, pidx := range idxs {
				if pidx < len(partNumbers) {
					out = append(out, partNumbers[pidx])
				}
			}
			return out
		}

		// Cross-check our verdict against MinIO's own before acting on ours. This
		// changes no behaviour; it exists to measure how often the count-based
		// accept disagrees with the committed-set intersection in production.
		if opt, pess := healableDivergence(placement, partInfoFiles, committed,
			fi.Erasure.DataBlocks); len(opt) > 0 || len(pess) > 0 {
			if len(opt) > 0 {
				storageLogIf(ctx, fmt.Errorf(
					"multipart healable-verdict DIVERGENCE on %s/%s: part(s) %v pass MinIO's readQuorum ETag count but hold fewer than %d data blocks among committed drives (held %#x, committed %#x); acting on the committed-set verdict. This is the count-based false-accept and it means a plain read of this object may fail",
					bucket, object, partNums(opt), fi.Erasure.DataBlocks, placement.held, committed))
			}
			if len(pess) > 0 {
				storageLogIf(ctx, fmt.Errorf(
					"multipart healable-verdict divergence on %s/%s: part(s) %v carry a readParts error yet hold at least %d data blocks among committed drives (held %#x, committed %#x); acting on the committed-set verdict",
					bucket, object, partNums(pess), fi.Erasure.DataBlocks, placement.held, committed))
			}
		}

		switch {
		case queue:
			storageLogIf(ctx, fmt.Errorf(
				"multipart write-set divergence on %s/%s: part(s) %v committed on fewer drives than the object's usable write set; queueing heal",
				bucket, object, partNums(underReplicatedParts)))
			er.addPartial(bucket, object, fi.VersionID)

		case len(lost) > 0:
			// Unreachable today: the commit-set collapse check above returns
			// InvalidPart before control gets here. Kept and logged because it is
			// unreachable only by that ordering, and if it ever fires the queue
			// would have handed MinIO an object it deletes rather than repairs.
			storageLogIf(ctx, fmt.Errorf(
				"multipart write-set divergence on %s/%s: part(s) %v are BELOW read quorum (%d data blocks needed); NOT queueing heal, because heal cannot repair a part below quorum and would evaluate the object for deletion instead. This object needs triage, not a heal",
				bucket, object, partNums(lost), fi.Erasure.DataBlocks))
		}
	}

	for i := range len(onlineDisks) {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	// we are adding a new version to this object under the namespace lock, so this is the latest version.
	fi.IsLatest = true

	// Success, return object info.
	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
func (er erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "AbortMultipartUpload", object, &er)
	}

	// Cleanup all uploaded parts.
	defer er.deleteAll(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID))

	// Validates if upload ID exists.
	_, _, err = er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	return toObjectErr(err, bucket, object, uploadID)
}
