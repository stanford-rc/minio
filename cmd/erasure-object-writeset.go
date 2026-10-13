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
	"context"
	"errors"
	"fmt"
	"sort"
)

// Write-set reporting for a single-part PUT.  The shortfall checks themselves
// are checkWriteSet in cmd/erasure-multipart.go, called from putObject after the
// shards are written ("encode") and after the commit ("commit").  This file
// holds the one finding that is not a shortfall.
//
// A drive that is already unavailable when a PUT starts is never attempted:
// putObject gives it no writer, and with the default availability-optimized
// storage class raises the object's parity for each such drive (putObject,
// minIOErasureUpgraded), so the write is degraded by design and its MRF heal is
// queued by the stock offline-drive loop after the commit.  Every PUT does this
// for as long as the drive is out, so the line names no object and goes through
// the log-once path, whose id is the text itself.  log-once remembers only the
// first text logged under an id, so a shared id would let one variant mask the
// others: a config save, which never raises parity, beside user PUTs that do.
// With the text as the id, each distinct drive set, count and parity outcome is
// logged once per node per hour.

// writeSetLogOnceIf is where the written-around line is logged.  A variable so
// tests can capture it; it is storageLogOnceIf in every build.
var writeSetLogOnceIf = storageLogOnceIf

// putObjectQueueHeal is where putObject queues the MRF heal for an object that
// is missing from a drive after its commit, the heal a single-part write-set
// line promises.  A variable so tests can see it queued; it is
// globalMRFState.addPartialOp in every build.
var putObjectQueueHeal = func(op PartialOperation) { globalMRFState.addPartialOp(op) }

// logWrittenAround logs a PUT that left out drives unavailable at its start.
// onlineDisks is the PUT's drive order; attemptedDisks holds the drives it gave
// a writer, nil elsewhere.  upgraded is the object's minIOErasureUpgraded value,
// "" when parity was not raised.
func (er erasureObjects) logWrittenAround(ctx context.Context, onlineDisks, attemptedDisks []StorageAPI, upgraded string) {
	if !multipartWriteSetEnabled() {
		return
	}
	var names []string
	missing := 0
	for i := range onlineDisks {
		if i < len(attemptedDisks) && attemptedDisks[i] != nil {
			continue
		}
		missing++
		if onlineDisks[i] != nil {
			names = append(names, onlineDisks[i].String())
		}
	}
	if missing == 0 {
		return
	}
	// Sorted, and with no object name, so the text is the same for every PUT
	// that writes around the same drives with the same parity outcome.
	sort.Strings(names)
	parity := "parity unchanged"
	if upgraded != "" {
		parity = "parity raised " + upgraded
	}
	msg := fmt.Sprintf(
		"single-part PUT writing around %d unavailable drive(s) in pool %d set %d %v (%d unnamed): %s; each object written this way is queued for heal at commit",
		missing, er.poolIndex, er.setIndex, names, missing-len(names), parity)
	writeSetLogOnceIf(ctx, errors.New(msg), "writeset-put-around: "+msg)
}
