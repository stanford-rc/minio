// Copyright (c) 2015-2024 MinIO, Inc.
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
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/minio/madmin-go/v3"
)

// Write-set reporting for a single-part PUT, the putObject counterpart of the
// multipart checks.  A single-part PUT can lose a drive at two points: while
// its shards are written ("encode"), and when its xl.meta is committed
// ("commit").  An inline object's data lives in xl.meta and is encoded into
// memory, so the commit is the only point at which it can lose a copy.

// putFaultDisk fails one drive's PUT operations on demand while reporting the
// drive online, which is the production shape of a lost shard.
type putFaultDisk struct {
	StorageAPI
	failCreate      atomic.Bool // CreateFile fails before reading anything
	failCreateLate  atomic.Bool // CreateFile reads every byte, then fails
	failRename      atomic.Bool // RenameData fails
	offline         atomic.Bool // the drive reports itself offline
	createCalls     atomic.Int64
	renameDataCalls atomic.Int64
}

func (d *putFaultDisk) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	d.createCalls.Add(1)
	if d.failCreate.Load() {
		return errFaultyDisk
	}
	if d.failCreateLate.Load() {
		// Drain the stream so every Write succeeds and the failure surfaces
		// only when the bitrot writer is closed.
		_, _ = io.Copy(io.Discard, reader)
		return errFaultyDisk
	}
	return d.StorageAPI.CreateFile(ctx, origvolume, volume, path, size, reader)
}

func (d *putFaultDisk) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo,
	dstVolume, dstPath string, opts RenameOptions,
) (RenameDataResp, error) {
	d.renameDataCalls.Add(1)
	if d.failRename.Load() {
		return RenameDataResp{}, errFaultyDisk
	}
	return d.StorageAPI.RenameData(ctx, srcVolume, srcPath, fi, dstVolume, dstPath, opts)
}

func (d *putFaultDisk) IsOnline() bool { return !d.offline.Load() }

// setupPutWriteSetTest returns an EC:1 four-drive object layer whose first drive
// is wrapped in a putFaultDisk.
func setupPutWriteSetTest(ctx context.Context, t *testing.T) (ObjectLayer, *putFaultDisk, []string) {
	t.Helper()
	pinParity(t, 4, 1)

	obj, fsDirs, err := prepareErasure(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "bucket", MakeBucketOptions{}); err != nil {
		removeRoots(fsDirs)
		t.Fatal(err)
	}

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]
	disks := xl.getDisks()
	w := &putFaultDisk{StorageAPI: disks[0]}
	z.serverPools[0].erasureDisksMu.Lock()
	disks[0] = w
	xl.getDisks = func() []StorageAPI { return disks }
	z.serverPools[0].erasureDisksMu.Unlock()
	return obj, w, fsDirs
}

// writeSetLines captures what the write-set code logs for the rest of the test.
type writeSetLines struct {
	mu    sync.Mutex
	lines []string
	once  []string // lines sent through the log-once path
}

func captureWriteSetLog(t *testing.T) *writeSetLines {
	t.Helper()
	c := &writeSetLines{}
	prev, prevOnce := writeSetLogIf, writeSetLogOnceIf
	writeSetLogIf = func(_ context.Context, err error, _ ...any) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lines = append(c.lines, err.Error())
	}
	writeSetLogOnceIf = func(_ context.Context, err error, id string, _ ...any) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.once = append(c.once, id+" | "+err.Error())
	}
	t.Cleanup(func() { writeSetLogIf, writeSetLogOnceIf = prev, prevOnce })
	return c
}

func (c *writeSetLines) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *writeSetLines) allOnce() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.once...)
}

// queuedHeals records the MRF heals putObject queues, in place of queueing
// them, so no background heal can change a drive while the test counts it.
type queuedHeals struct {
	mu  sync.Mutex
	ops []PartialOperation
}

func captureQueuedHeals(t *testing.T) *queuedHeals {
	t.Helper()
	q := &queuedHeals{}
	prev := putObjectQueueHeal
	putObjectQueueHeal = func(op PartialOperation) {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.ops = append(q.ops, op)
	}
	t.Cleanup(func() { putObjectQueueHeal = prev })
	return q
}

// requireQueued fails unless exactly the named objects were queued for heal,
// in order.
func (q *queuedHeals) requireQueued(t *testing.T, objects ...string) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	var got []string
	for _, op := range q.ops {
		got = append(got, op.Bucket+"/"+op.Object)
	}
	want := make([]string, len(objects))
	for i, o := range objects {
		want[i] = "bucket/" + o
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("queued heals: want %q, got %q", want, got)
	}
}

// Sizes either side of the inline threshold.  An EC:1 object is inline when its
// shard is at most 128 KiB, so 1 KiB is inline and 1 MiB is not.
const (
	inlinePutSize  = 1 << 10
	regularPutSize = 1 << 20
)

func putSingle(ctx context.Context, t *testing.T, obj ObjectLayer, object string, size int) (ObjectInfo, error) {
	t.Helper()
	data := bytes.Repeat([]byte("p"), size)
	return obj.PutObject(ctx, "bucket", object,
		mustGetPutObjReader(t, bytes.NewReader(data), int64(size), "", ""), ObjectOptions{})
}

func requireOneLine(t *testing.T, lines []string, want ...string) {
	t.Helper()
	if len(lines) != 1 {
		t.Fatalf("want exactly one write-set line, got %d: %q", len(lines), lines)
	}
	for _, w := range want {
		if !strings.Contains(lines[0], w) {
			t.Fatalf("write-set line lacks %q:\n%s", w, lines[0])
		}
	}
}

func TestPutObjectEncodeShortfallIsLoggedAndHealable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	heals := captureQueuedHeals(t)

	w.failCreate.Store(true)
	if _, err := putSingle(ctx, t, obj, "regular", regularPutSize); err != nil {
		t.Fatalf("a healable shortfall must be accepted: %v", err)
	}
	w.failCreate.Store(false)
	if w.createCalls.Load() == 0 {
		t.Fatal("the faulted drive was never asked to CreateFile, so this case proved nothing")
	}

	requireOneLine(t, log.all(),
		"single-part PUT write-set shortfall at encode on bucket/regular: landed on 3 of 4 attempted drives",
		"lost healthy [", "heal queued at commit")
	heals.requireQueued(t, "regular")
	if strings.Contains(log.all()[0], "(inline)") {
		t.Fatal("a 1 MiB object is not inline")
	}
	if got := countCommittedObjectDirs(t, fsDirs, "part.1"); got != 3 {
		t.Fatalf("part.1 should be on 3 of 4 drives after the shortfall, got %d", got)
	}

	if _, err := obj.HealObject(ctx, "bucket", "regular", "", madmin.HealOpts{ScanMode: madmin.HealNormalScan}); err != nil {
		t.Fatalf("HealObject: %v", err)
	}
	if got := countCommittedObjectDirs(t, fsDirs, "part.1"); got != 4 {
		t.Fatalf("heal must restore part.1 to all 4 drives, got %d", got)
	}
}

func TestPutObjectInlineCommitShortfallIsLoggedAndHealable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	heals := captureQueuedHeals(t)

	w.failRename.Store(true)
	if _, err := putSingle(ctx, t, obj, "small", inlinePutSize); err != nil {
		t.Fatalf("a healable shortfall must be accepted: %v", err)
	}
	w.failRename.Store(false)

	requireOneLine(t, log.all(),
		"single-part PUT write-set shortfall at commit on bucket/small (inline): landed on 3 of 4 attempted drives",
		"heal queued at commit")
	heals.requireQueued(t, "small")
	if got := countCommittedObjectDirs(t, fsDirs, "xl.meta"); got != 3 {
		t.Fatalf("xl.meta should be on 3 of 4 drives after the shortfall, got %d", got)
	}
	if got := countCommittedObjectDirs(t, fsDirs, "part.1"); got != 0 {
		t.Fatalf("an inline object has no part files, found %d", got)
	}

	if _, err := obj.HealObject(ctx, "bucket", "small", "", madmin.HealOpts{ScanMode: madmin.HealNormalScan}); err != nil {
		t.Fatalf("HealObject: %v", err)
	}
	if got := countCommittedObjectDirs(t, fsDirs, "xl.meta"); got != 4 {
		t.Fatalf("heal must restore xl.meta to all 4 drives, got %d", got)
	}
}

func TestPutObjectRegularCommitShortfallIsLogged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	heals := captureQueuedHeals(t)

	w.failRename.Store(true)
	if _, err := putSingle(ctx, t, obj, "regular", regularPutSize); err != nil {
		t.Fatalf("a healable shortfall must be accepted: %v", err)
	}
	requireOneLine(t, log.all(),
		"single-part PUT write-set shortfall at commit on bucket/regular: landed on 3 of 4 attempted drives")
	heals.requireQueued(t, "regular")
}

// A drive whose CreateFile takes every byte and then fails is reported at
// encode or at commit, depending on a race in the streaming writer: its Close
// reports the ring buffer's error, which is CreateFile's only if CreateFile
// returns before Close finishes flushing.  Otherwise the drive passes encode,
// has no shard to commit, and fails RenameData.  Either way it is reported
// exactly once, queued for heal, and the object reads back intact.
func TestPutObjectLateCreateFailureIsReported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	// Recording the heal instead of queueing it also keeps the background heal
	// routine from calling RenameData on the drive.
	heals := captureQueuedHeals(t)

	w.failCreateLate.Store(true)
	if _, err := putSingle(ctx, t, obj, "regular", regularPutSize); err != nil {
		t.Fatalf("a healable shortfall must be accepted: %v", err)
	}
	if w.createCalls.Load() == 0 {
		t.Fatal("the faulted drive was never asked to CreateFile, so this case proved nothing")
	}
	stage, calls := "commit", int64(1)
	if l := log.all(); len(l) == 1 && strings.Contains(l[0], " at encode ") {
		stage, calls = "encode", 0
	}
	requireOneLine(t, log.all(),
		"single-part PUT write-set shortfall at "+stage+" on bucket/regular: landed on 3 of 4 attempted drives")
	if got := w.renameDataCalls.Load(); got != calls {
		t.Fatalf("reported at %s, so the drive should reach the commit %d time(s), got %d", stage, calls, got)
	}
	heals.requireQueued(t, "regular")

	r, err := obj.GetObjectNInfo(ctx, "bucket", "regular", nil, nil, ObjectOptions{})
	if err != nil {
		t.Fatalf("GetObjectNInfo: %v", err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || len(got) != regularPutSize {
		t.Fatalf("read back %d bytes, err %v", len(got), err)
	}
}

func TestPutObjectCleanWritesLogNothing(t *testing.T) {
	for _, size := range []int{inlinePutSize, regularPutSize} {
		ctx, cancel := context.WithCancel(context.Background())
		obj, _, fsDirs := setupPutWriteSetTest(ctx, t)
		log := captureWriteSetLog(t)
		if _, err := putSingle(ctx, t, obj, "clean", size); err != nil {
			t.Fatalf("clean PUT of %d bytes: %v", size, err)
		}
		if l := append(log.all(), log.allOnce()...); len(l) != 0 {
			t.Fatalf("a clean %d-byte PUT logged %q", size, l)
		}
		obj.Shutdown(context.Background())
		removeRoots(fsDirs)
		cancel()
	}
}

// The switch silences every single-part write-set line, at encode, at commit
// and for a drive written around.  MinIO's own heal queueing is not the
// switch's to turn off, so each of those PUTs is still queued for heal.
func TestPutObjectWriteSetOffIsSilent(t *testing.T) {
	t.Setenv("MINIO_MULTIPART_WRITESET", "off")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	heals := captureQueuedHeals(t)

	cases := []struct {
		object string
		size   int
		fault  *atomic.Bool
	}{
		{"encode", regularPutSize, &w.failCreate},
		{"commit-inline", inlinePutSize, &w.failRename},
		{"commit-regular", regularPutSize, &w.failRename},
		{"around", regularPutSize, &w.offline},
	}
	var objects []string
	for _, c := range cases {
		c.fault.Store(true)
		_, err := putSingle(ctx, t, obj, c.object, c.size)
		c.fault.Store(false)
		if err != nil {
			t.Fatalf("PUT %s with the switch off: %v", c.object, err)
		}
		objects = append(objects, c.object)
	}
	if w.createCalls.Load() == 0 || w.renameDataCalls.Load() == 0 {
		t.Fatal("the faulted drive was never asked to write, so this case proved nothing")
	}
	if l := append(log.all(), log.allOnce()...); len(l) != 0 {
		t.Fatalf("MINIO_MULTIPART_WRITESET=off must silence every write-set line, got %q", l)
	}
	heals.requireQueued(t, objects...)
}

// A drive offline when the PUT starts is never attempted.  That is not a
// shortfall; it is logged through the log-once path, with the same text and id
// for every object, so an outage costs one line an hour rather than one per PUT.
func TestPutObjectWrittenAroundAnOfflineDrive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	heals := captureQueuedHeals(t)

	w.offline.Store(true)
	for _, name := range []string{"first", "second"} {
		if _, err := putSingle(ctx, t, obj, name, regularPutSize); err != nil {
			t.Fatalf("PUT %s with one drive offline: %v", name, err)
		}
	}
	if l := log.all(); len(l) != 0 {
		t.Fatalf("an offline drive is not a shortfall, got %q", l)
	}
	once := log.allOnce()
	if len(once) != 2 {
		t.Fatalf("want one written-around report per PUT, got %q", once)
	}
	if once[0] != once[1] {
		t.Fatalf("the reports must be identical so log-once suppresses them:\n%s\n%s", once[0], once[1])
	}
	for _, want := range []string{"writing around 1 unavailable drive(s) in pool 0 set 0", "parity raised 1->2", "queued for heal at commit"} {
		if !strings.Contains(once[0], want) {
			t.Fatalf("written-around report lacks %q:\n%s", want, once[0])
		}
	}
	id, text, _ := strings.Cut(once[0], " | ")
	if id != "writeset-put-around: "+text {
		t.Fatalf("the log-once id must be the text, so no variant masks another:\nid   %s\ntext %s", id, text)
	}
	if strings.Contains(once[0], "first") || strings.Contains(once[0], "second") {
		t.Fatalf("the report must not name the object:\n%s", once[0])
	}
	heals.requireQueued(t, "first", "second")
}

// log-once keeps only the first text logged under an id, so a PUT that writes
// around the same drive with a different parity outcome must use a different
// id.  A config save is written at maximum parity and never raises it.
func TestPutObjectWrittenAroundVariantsAreNotMasked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, w, fsDirs := setupPutWriteSetTest(ctx, t)
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)
	log := captureWriteSetLog(t)
	captureQueuedHeals(t)

	w.offline.Store(true)
	data := bytes.Repeat([]byte("p"), regularPutSize)
	if _, err := obj.PutObject(ctx, "bucket", "config", mustGetPutObjReader(t, bytes.NewReader(data),
		int64(len(data)), "", ""), ObjectOptions{MaxParity: true}); err != nil {
		t.Fatalf("max-parity PUT with one drive offline: %v", err)
	}
	if _, err := putSingle(ctx, t, obj, "user", regularPutSize); err != nil {
		t.Fatalf("PUT with one drive offline: %v", err)
	}
	once := log.allOnce()
	if len(once) != 2 {
		t.Fatalf("want one written-around report per PUT, got %q", once)
	}
	if !strings.Contains(once[0], "parity unchanged") || !strings.Contains(once[1], "parity raised 1->2") {
		t.Fatalf("want the max-parity PUT unchanged and the user PUT raised:\n%s\n%s", once[0], once[1])
	}
	id0, _, _ := strings.Cut(once[0], " | ")
	id1, _, _ := strings.Cut(once[1], " | ")
	if id0 == id1 {
		t.Fatalf("different reports share the log-once id %q, so the second would be masked", id0)
	}
}

// An S3 key may contain '%'.  The key is data in the log line, never format.
func TestWriteSetSubjectIsNotAFormatString(t *testing.T) {
	got := writeSetOp{singlePut: true}.subject("commit", "bucket", "100%s%d")
	if got != "single-part PUT write-set shortfall at commit on bucket/100%s%d" {
		t.Fatalf("got %q", got)
	}
}

// The multipart lines go through the same code as the single-part ones, and
// Splunk searches and alerts match on their text, so it is pinned exactly.
func TestMultipartShortfallLogTextIsUnchanged(t *testing.T) {
	const tail = "); healable, accepted, commit-boundary detection will queue the heal"

	t.Run("encode", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		obj, wrapped, fsDirs := setupWriteSetTest(ctx, t, 1)
		defer obj.Shutdown(context.Background())
		defer removeRoots(fsDirs)
		log := captureWriteSetLog(t)

		wrapped[0].failing.Store(true)
		if err := putOnePart(ctx, t, obj, "mpu"); err != nil {
			t.Fatalf("PutObjectPart: %v", err)
		}
		lines := log.all()
		requireOneLine(t, lines,
			"multipart write-set shortfall at encode on bucket/mpu part 1: landed on 3 of 4 attempted drives (lost healthy [")
		if !strings.HasSuffix(lines[0], "], lost unhealthy []"+tail) {
			t.Fatalf("multipart line text changed:\n%s", lines[0])
		}
	})

	t.Run("rename", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		obj, wrapped, fsDirs := setupRenamePartTest(ctx, t, 1)
		defer obj.Shutdown(context.Background())
		defer removeRoots(fsDirs)
		log := captureWriteSetLog(t)

		wrapped[0].failing.Store(true)
		if err := putOnePart(ctx, t, obj, "mpu"); err != nil {
			t.Fatalf("PutObjectPart: %v", err)
		}
		lines := log.all()
		requireOneLine(t, lines,
			"multipart write-set shortfall at rename on bucket/mpu part 1: landed on 3 of 4 attempted drives (lost healthy [")
		if !strings.HasSuffix(lines[0], "], lost unhealthy []"+tail) {
			t.Fatalf("multipart line text changed:\n%s", lines[0])
		}
	})
}

// The refusal line keeps its multipart wording, and says "the object" for a
// single-part PUT.
func TestWriteSetRefusalNamesTheWrite(t *testing.T) {
	ctx := context.Background()
	er := erasureObjects{}
	log := captureWriteSetLog(t)
	disks := []StorageAPI{nil, nil, nil, nil}
	_ = er.checkWriteSet(ctx, "encode", "b", "o", writeSetOp{part: 7}, disks, disks, 1, 4, 3)
	_ = er.checkWriteSet(ctx, "encode", "b", "o", writeSetOp{singlePut: true}, disks, disks, 1, 4, 3)
	lines := log.all()
	if len(lines) != 2 {
		t.Fatalf("want two refusal lines, got %q", lines)
	}
	if !strings.HasPrefix(lines[0], "multipart write-set shortfall at encode on b/o part 7: landed on 1 of 4 attempted drives") ||
		!strings.HasSuffix(lines[0], "UNRECOVERABLE, refusing the part because it could not be reconstructed from parity") {
		t.Fatalf("multipart refusal text changed:\n%s", lines[0])
	}
	if !strings.HasSuffix(lines[1], "UNRECOVERABLE, refusing the object because it could not be reconstructed from parity") {
		t.Fatalf("single-part refusal text:\n%s", lines[1])
	}
}
