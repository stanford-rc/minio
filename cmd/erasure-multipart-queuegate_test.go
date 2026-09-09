// Elm addition, 2026-08-31.  Recommendation 3: refuse to queue a heal for
// a part below read quorum.
//
// WHY A PREDICATE RATHER THAN A CHECK.  The below-quorum case was already
// unreachable at the queueing site, but only emergently, from two facts that live
// elsewhere: readParts gates both `underReplicated` and `partPlacement.valid` on
// the same trackSets flag, and the commit-set collapse check returns InvalidPart
// earlier in CompleteMultipartUpload.  Nothing at the queueing site said so, so a
// reorder or a new return path would have reopened the hole silently.
//
// WHAT THE HOLE WOULD COST.  Heal cannot repair a part below read quorum: master's
// healObject sets cannotHeal on exactly that condition and calls deleteIfDangling,
// which removes the whole object VERSION from every drive.  One part below quorum
// in a 203-part object destroys all 203, including the ~200 intact ones, and that
// path is gated on neither opts.Remove nor a scan mode.  So queueing such an object
// does not merely waste MRF capacity; it hands MinIO an object it deletes.

package cmd

import "testing"

func TestShouldQueueWriteSetHealQueuesARecoverableShortfall(t *testing.T) {
	// Four drives, EC:1, so dataBlocks is 3.  The part is held on three of the four
	// committed drives: under-replicated relative to the usable set, still readable.
	// This is the case Fix A exists for and it must queue.
	p := partPlacement{
		held:   []uint64{bit(0, 1, 2)},
		usable: bit(0, 1, 2, 3),
		valid:  true,
	}
	queue, lost := shouldQueueWriteSetHeal(p, []int{0}, bit(0, 1, 2, 3), 3)
	if !queue {
		t.Fatal("a part at exactly read quorum is repairable and must be queued")
	}
	if len(lost) != 0 {
		t.Fatalf("nothing is below quorum here, got lost=%v", lost)
	}
}

func TestShouldQueueWriteSetHealRefusesBelowQuorum(t *testing.T) {
	// Same geometry, but the part is held on only two of the committed drives.
	// Two is below the three needed, so it cannot be reconstructed. Queueing it
	// would hand MinIO an object it deletes rather than repairs.
	p := partPlacement{
		held:   []uint64{bit(0, 1)},
		usable: bit(0, 1, 2, 3),
		valid:  true,
	}
	queue, lost := shouldQueueWriteSetHeal(p, []int{0}, bit(0, 1, 2, 3), 3)
	if queue {
		t.Fatal("a part below read quorum must NOT be queued: heal cannot repair it " +
			"and deleteIfDangling would remove the whole object version, taking " +
			"every intact part with it")
	}
	if len(lost) != 1 || lost[0] != 0 {
		t.Fatalf("the refusal must name the part so the log can explain itself, got %v",
			lost)
	}
}

func TestShouldQueueWriteSetHealRefusesWhenOnlyOnePartIsBelowQuorum(t *testing.T) {
	// The production shape: one bad part among many good ones. The whole object is
	// withheld from the queue, because deleteIfDangling operates on the VERSION and
	// would destroy the good parts alongside the bad one.
	p := partPlacement{
		held: []uint64{
			bit(0, 1, 2, 3), // fine
			bit(0, 1, 2),    // under-replicated, repairable
			bit(2, 3),       // BELOW quorum
		},
		usable: bit(0, 1, 2, 3),
		valid:  true,
	}
	queue, lost := shouldQueueWriteSetHeal(p, []int{1, 2}, bit(0, 1, 2, 3), 3)
	if queue {
		t.Fatal("one part below quorum must withhold the whole object from the queue")
	}
	if len(lost) != 1 || lost[0] != 2 {
		t.Fatalf("expected only part index 2 reported as lost, got %v", lost)
	}
}

func TestShouldQueueWriteSetHealNoShortfallNoQueue(t *testing.T) {
	p := partPlacement{
		held:   []uint64{bit(0, 1, 2, 3)},
		usable: bit(0, 1, 2, 3),
		valid:  true,
	}
	if queue, _ := shouldQueueWriteSetHeal(p, nil, bit(0, 1, 2, 3), 3); queue {
		t.Fatal("no shortfall means nothing to queue")
	}
}

func TestShouldQueueWriteSetHealUntrackedPlacementDoesNotQueue(t *testing.T) {
	// The coupling that made the hole unreachable before, now asserted locally.
	//
	// readParts gates BOTH `underReplicated` and `partPlacement.valid` on the same
	// trackSets flag, so when the drive set is too wide to track it reports no
	// shortfall and the predicate has nothing to act on.
	p := partPlacement{
		held:   []uint64{bit(0, 1)}, // below quorum, but unusable
		usable: bit(0, 1, 2, 3),
		valid:  false,
	}
	if queue, _ := shouldQueueWriteSetHeal(p, nil, bit(0, 1, 2, 3), 3); queue {
		t.Fatal("an untracked drive set reports no shortfall and must not queue")
	}
}

func TestUnreadableAfterCannotJudgeAnUntrackedPlacement(t *testing.T) {
	// Why the coupling above has to hold, stated as its own test because it is the
	// hazard rather than the behaviour.
	//
	// unreadableAfter returns nil on an invalid placement, by design: it would
	// rather skip than be wrong. So if `underReplicated` were ever reported for an
	// invalid placement, the below-quorum test would silently pass and the object
	// would be queued blind. The two must stay gated on the same flag.
	p := partPlacement{
		held:   []uint64{bit(0, 1)}, // two of three needed: below quorum
		usable: bit(0, 1, 2, 3),
		valid:  false,
	}
	if lost := p.unreadableAfter(bit(0, 1, 2, 3), 3); len(lost) != 0 {
		t.Fatalf("unreadableAfter must skip an invalid placement, got %v", lost)
	}
	// Decoupled, the predicate would queue a lost object. This asserts the shape of
	// the hazard so the coupling is not quietly removed as redundant.
	queue, _ := shouldQueueWriteSetHeal(p, []int{0}, bit(0, 1, 2, 3), 3)
	if !queue {
		t.Fatal("expected the predicate to be blind here; if it now consults " +
			"placement validity directly, the coupling in readParts is no longer " +
			"load-bearing and this test should be replaced")
	}
}

func TestShouldQueueWriteSetHealUsesTheCommittedSetNotTheUsableSet(t *testing.T) {
	// The distinction that decides the verdict. The part is on drives 0,1,2, which
	// is quorum against the usable set, but drive 2 did not take the commit. Against
	// the committed set it is on two drives, which is below quorum, so this must
	// refuse. Evaluating against `usable` instead would queue a lost object.
	p := partPlacement{
		held:   []uint64{bit(0, 1, 2)},
		usable: bit(0, 1, 2, 3),
		valid:  true,
	}
	committed := bit(0, 1, 3) // drive 2 excluded from the commit
	queue, lost := shouldQueueWriteSetHeal(p, []int{0}, committed, 3)
	if queue {
		t.Fatal("quorum must be judged against the COMMITTED drives, not the drives " +
			"usable at read time: a part on a drive that did not take the commit is " +
			"not part of the object")
	}
	if len(lost) != 1 {
		t.Fatalf("expected the part reported lost, got %v", lost)
	}
}
