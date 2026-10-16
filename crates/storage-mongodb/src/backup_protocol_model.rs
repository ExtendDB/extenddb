// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! An exhaustive model of the restore / DeleteBackup claim protocol, with the
//! heartbeat sweep as an adversary. It is a model of the protocol, not a test
//! of the code: each step stands for one MongoDB operation in
//! `register_restore`, `copy_restore`, `delete_backup`, or
//! `sweep_stale_backup_claims`, except `RRelease`, and the model is kept in
//! step with them by hand. `RRelease` stands for up to three operations of
//! `register_restore` run as one atomic step: the claim release (an
//! `update_one`), the lost-claim recheck of the backup document (a `find_one`,
//! only when the release matched nothing), and the abort of the target
//! (`abort_backup_restore`, only when the recheck finds the backup gone or
//! delete-claimed). Interleavings between those three are not explored.
//!
//! Threads, each a fixed sequence of steps; every interleaving of them is
//! checked:
//!
//! - two restores of the same backup into different targets: claim (the second
//!   restore retries once, as `register_restore` retries a claim held by another
//!   restore), insert the CREATING target, release the claim (with the
//!   lost-claim recheck), read the backup's data (`$out`, as one read), hand the
//!   target over, and the backfill worker flipping it ACTIVE. The retry also
//!   happens when the first claim found a delete claim, where the code ends the
//!   restore with BackupInUse at once; that allows more interleavings than the
//!   code has;
//! - DeleteBackup: target check, claim, recheck, drop the data, mark DELETED;
//! - DeleteBackup stopping at any point (its request future dropped, or an
//!   error returned): its claim is released only if it is still armed, and it
//!   is disarmed before the drop;
//! - sweep events that act on a live owner whose heartbeat lapsed (a stalled
//!   process): clearing the restore claim (twice), removing each CREATING
//!   target that was not handed over;
//! - the sweep's delete takeover (take over a stale delete claim, check for a
//!   restore target, drop, mark DELETED), and that takeover stopping at any
//!   point. The sweep never releases a delete claim it took over. The code
//!   also checks for a CREATING target before taking over and leaves the claim
//!   alone when it finds one; the model has no such check and always takes
//!   over, which allows more interleavings than the code has, so the model
//!   does not exercise that check.
//!
//! Sweep events may fire at any point, whether or not the owner is stalled,
//! which over-approximates what the sweep can do.
//!
//! Not modelled: `$out` as a non-atomic read, more than one DeleteBackup, more
//! than one delete takeover, the timing of heartbeats (staleness is assumed
//! possible at every step), and a release that fails. In the model every
//! release succeeds. In the code:
//!
//! - DeleteBackup's `release()` after its recheck found a target can return an
//!   error; the request then fails with the claim still armed, its `Drop`
//!   spawns a second release, and if that fails too the claim stays until the
//!   sweep finishes the delete, which it does only once no CREATING target of
//!   the backup exists (so a DeleteBackup that returned an error can still
//!   take effect later);
//! - a restore's release in `hand_claim_to_target` can return an error, and so
//!   can the lost-claim recheck; the restore then aborts its target (if the
//!   abort fails, the sweep removes the target once its heartbeat is stale)
//!   and its still-armed claim is released by `Drop` or, if that fails, cleared
//!   by the sweep;
//! - the releases on a restore's earlier failure paths (no `backup_id`, target
//!   creation failed before the hand-over) log an error and leave the same
//!   `Drop` / sweep backstop.
//!
//! Invariants, checked after every step: no restore claim and delete claim are
//! held together; no restore is handed over after reading dropped data; the
//! backup is never AVAILABLE with no claim after its data was dropped (a later
//! restore would copy nothing). Liveness: some interleaving completes a restore
//! and the delete, some completes both restores, and without stops and sweep
//! events no interleaving leaves a claim behind.
//!
//! The checker explores reachable states once each (depth first, memoised on
//! the state and every thread's position) and counts the interleavings below
//! each state, so the count is the number of interleavings covered without
//! enumerating them. A brute-force enumerator cross-checks it on smaller
//! configurations, including the 39,060 interleavings of the previous model.
//!
//! The four tests that explore the full model take about 25 s each in a debug
//! build and are `#[ignore]`d, so `cargo test` stays quick; run them with
//! `cargo test -p extenddb-storage-mongodb --lib protocol_model -- --include-ignored`
//! after any change to the claim protocol or to this model. The three quick
//! tests (the 20-ordering model, the brute-force cross-check, and the
//! no-stops liveness check) run by default.

use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    // Restore `i` into target `i`. `RClaim(i, last)`: a failed claim ends the
    // restore only when `last`.
    RClaim(usize, bool),
    RInsertTarget(usize),
    RRelease(usize),
    RCopyRead(usize),
    RHandOver(usize),
    RActivate(usize),
    // DeleteBackup
    DCheckTarget,
    DClaim,
    DRecheckTarget,
    DDropData,
    DMarkDeleted,
    /// The DeleteBackup request stops: future dropped, or an error returned
    /// (including an ambiguous error from the drop itself).
    DStop,
    // Sweep acting on a stalled live owner
    SClearRestoreClaim,
    SRemoveTarget(usize),
    // Sweep step 3
    STakeOverDelete,
    SCheckTarget,
    SDropData,
    SMarkDeleted,
    /// The sweep's step 3 returns early (an error from any of its operations).
    SStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Target {
    None,
    Creating,
    /// CREATING with `restore_backfill_pending`: still matched by
    /// `restoring_target_filter`, no longer by the sweep.
    HandedOver,
    Active,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum DeleteOwner {
    Request,
    Sweep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Restore {
    claimed: bool,
    target: Target,
    copy_saw_data: Option<bool>,
    done: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct World {
    available: bool,
    data_present: bool,
    restore_claim: Option<usize>,
    delete_claim: Option<DeleteOwner>,
    restores: [Restore; 2],
    d_done: bool,
    /// DeleteBackup's `BackupMetadataClaim` would release on drop/error.
    d_armed: bool,
    s_active: bool,
    s_done: bool,
    /// The sweep's taken-over claim would be released on an early return.
    s_armed: bool,
}

impl World {
    fn initial() -> Self {
        let restore = Restore {
            claimed: false,
            target: Target::None,
            copy_saw_data: None,
            done: false,
        };
        Self {
            available: true,
            data_present: true,
            restore_claim: None,
            delete_claim: None,
            restores: [restore; 2],
            d_done: false,
            d_armed: false,
            s_active: false,
            s_done: false,
            s_armed: false,
        }
    }

    /// `restoring_target_filter`: any CREATING target, handed over or not.
    fn restoring_target(&self) -> bool {
        self.restores
            .iter()
            .any(|r| matches!(r.target, Target::Creating | Target::HandedOver))
    }
}

/// Earlier revisions of the code, to show the model notices each of them.
#[derive(Clone, Copy, Debug, Default)]
struct Variant {
    /// B1 before the fix: DeleteBackup's claim (and the sweep's taken-over
    /// claim) stays armed across the drop, so a stop after the drop releases it.
    release_after_drop: bool,
    /// The sweep before this fix: it released a taken-over delete claim when it
    /// found a restore target, and its armed claim released on an early return.
    sweep_releases: bool,
    /// `register_restore` without its recheck after a lost claim.
    no_lost_claim_recheck: bool,
}

fn apply(w: &mut World, step: Step, v: Variant) -> Result<(), String> {
    match step {
        Step::RClaim(i, last) => {
            let r = &mut w.restores[i];
            if !r.done && !r.claimed {
                if w.available && w.restore_claim.is_none() && w.delete_claim.is_none() {
                    w.restore_claim = Some(i);
                    r.claimed = true;
                } else if last {
                    r.done = true;
                }
            }
        }
        Step::RInsertTarget(i) => {
            if !w.restores[i].done {
                w.restores[i].target = Target::Creating;
            }
        }
        Step::RRelease(i) => {
            if !w.restores[i].done {
                if w.restore_claim == Some(i) {
                    w.restore_claim = None;
                } else if !v.no_lost_claim_recheck && !(w.available && w.delete_claim.is_none()) {
                    // Claim lost and the backup is gone or being deleted:
                    // abort the target.
                    let r = &mut w.restores[i];
                    if r.target == Target::Creating {
                        r.target = Target::None;
                    }
                    r.done = true;
                }
            }
        }
        Step::RCopyRead(i) => {
            if !w.restores[i].done {
                w.restores[i].copy_saw_data = Some(w.data_present);
            }
        }
        Step::RHandOver(i) => {
            let r = &mut w.restores[i];
            if !r.done {
                r.done = true;
                if r.target == Target::Creating {
                    r.target = Target::HandedOver;
                    if r.copy_saw_data != Some(true) {
                        return Err(format!(
                            "a restore completed from dropped backup data: {w:?}"
                        ));
                    }
                }
            }
        }
        Step::RActivate(i) => {
            if w.restores[i].target == Target::HandedOver {
                w.restores[i].target = Target::Active;
            }
        }
        Step::DCheckTarget => {
            if !w.d_done && w.restoring_target() {
                w.d_done = true;
            }
        }
        Step::DClaim => {
            if !w.d_done {
                if w.available && w.restore_claim.is_none() && w.delete_claim.is_none() {
                    w.delete_claim = Some(DeleteOwner::Request);
                    w.d_armed = true;
                } else {
                    w.d_done = true;
                }
            }
        }
        Step::DRecheckTarget => {
            if !w.d_done && w.restoring_target() {
                // release() matches the operation id.
                if w.delete_claim == Some(DeleteOwner::Request) {
                    w.delete_claim = None;
                }
                w.d_armed = false;
                w.d_done = true;
            }
        }
        Step::DDropData => {
            if !w.d_done {
                if !v.release_after_drop {
                    w.d_armed = false;
                }
                w.data_present = false;
            }
        }
        Step::DMarkDeleted => {
            if !w.d_done {
                if w.delete_claim == Some(DeleteOwner::Request) {
                    w.available = false;
                    w.delete_claim = None;
                }
                w.d_armed = false;
                w.d_done = true;
            }
        }
        Step::DStop => {
            if !w.d_done {
                w.d_done = true;
                if w.d_armed && w.delete_claim == Some(DeleteOwner::Request) {
                    w.delete_claim = None;
                }
                w.d_armed = false;
            }
        }
        Step::SClearRestoreClaim => w.restore_claim = None,
        Step::SRemoveTarget(i) => {
            if w.restores[i].target == Target::Creating {
                w.restores[i].target = Target::None;
            }
        }
        Step::STakeOverDelete => {
            if !w.s_done {
                if w.delete_claim == Some(DeleteOwner::Request) {
                    w.delete_claim = Some(DeleteOwner::Sweep);
                    w.s_active = true;
                    w.s_armed = v.sweep_releases;
                } else {
                    w.s_done = true;
                }
            }
        }
        Step::SCheckTarget => {
            if w.s_active && !w.s_done && w.restoring_target() {
                if v.sweep_releases && w.delete_claim == Some(DeleteOwner::Sweep) {
                    w.delete_claim = None;
                }
                // Otherwise the claim is left for a later sweep.
                w.s_armed = false;
                w.s_done = true;
            }
        }
        Step::SDropData => {
            if w.s_active && !w.s_done {
                if !v.release_after_drop {
                    w.s_armed = false;
                }
                w.data_present = false;
            }
        }
        Step::SMarkDeleted => {
            if w.s_active && !w.s_done {
                if w.delete_claim == Some(DeleteOwner::Sweep) {
                    w.available = false;
                    w.delete_claim = None;
                }
                w.s_armed = false;
                w.s_done = true;
            }
        }
        Step::SStop => {
            if w.s_active && !w.s_done {
                w.s_done = true;
                if w.s_armed && w.delete_claim == Some(DeleteOwner::Sweep) {
                    w.delete_claim = None;
                }
                w.s_armed = false;
            }
        }
    }
    if w.restore_claim.is_some() && w.delete_claim.is_some() {
        return Err(format!("both claims held: {w:?}"));
    }
    if w.available && !w.data_present && w.delete_claim.is_none() {
        return Err(format!(
            "backup AVAILABLE and unclaimed after its data was dropped: {w:?}"
        ));
    }
    Ok(())
}

const MAX_THREADS: usize = 12;

#[derive(Debug, Default)]
struct Exploration {
    /// Distinct (state, thread positions) explored.
    states: usize,
    /// Interleavings covered (every complete schedule of the threads).
    interleavings: u128,
    /// One entry per violating transition, with the schedule that reached it.
    violations: Vec<String>,
    /// Final states of complete schedules.
    terminals: HashSet<World>,
}

fn explore(threads: &[&[Step]], v: Variant) -> Exploration {
    assert!(threads.len() <= MAX_THREADS);
    struct Ctx<'a> {
        threads: &'a [&'a [Step]],
        v: Variant,
        memo: HashMap<(World, [u8; MAX_THREADS]), u128>,
        out: Exploration,
        path: Vec<Step>,
    }
    fn go(ctx: &mut Ctx<'_>, w: World, pcs: [u8; MAX_THREADS]) -> u128 {
        if let Some(&n) = ctx.memo.get(&(w, pcs)) {
            return n;
        }
        let mut total = 0_u128;
        let mut any = false;
        for t in 0..ctx.threads.len() {
            let pc = usize::from(pcs[t]);
            let Some(&step) = ctx.threads[t].get(pc) else {
                continue;
            };
            any = true;
            let mut next = w;
            ctx.path.push(step);
            match apply(&mut next, step, ctx.v) {
                Ok(()) => {
                    let mut next_pcs = pcs;
                    next_pcs[t] += 1;
                    total += go(ctx, next, next_pcs);
                }
                Err(e) => ctx.out.violations.push(format!("{:?}: {e}", ctx.path)),
            }
            ctx.path.pop();
        }
        if !any {
            ctx.out.terminals.insert(w);
            total = 1;
        }
        ctx.memo.insert((w, pcs), total);
        total
    }
    let mut ctx = Ctx {
        threads,
        v,
        memo: HashMap::new(),
        out: Exploration::default(),
        path: Vec::new(),
    };
    let n = go(&mut ctx, World::initial(), [0; MAX_THREADS]);
    ctx.out.interleavings = n;
    ctx.out.states = ctx.memo.len();
    ctx.out
}

/// Every interleaving of the given sequences, each kept in order.
fn interleavings(seqs: &[&[Step]]) -> Vec<Vec<Step>> {
    fn go(seqs: &[&[Step]], idx: &mut Vec<usize>, cur: &mut Vec<Step>, out: &mut Vec<Vec<Step>>) {
        let mut any = false;
        for i in 0..seqs.len() {
            if idx[i] < seqs[i].len() {
                any = true;
                cur.push(seqs[i][idx[i]]);
                idx[i] += 1;
                go(seqs, idx, cur, out);
                idx[i] -= 1;
                cur.pop();
            }
        }
        if !any {
            out.push(cur.clone());
        }
    }
    let mut out = Vec::new();
    go(seqs, &mut vec![0; seqs.len()], &mut Vec::new(), &mut out);
    out
}

/// Run one interleaving; `Err` names the first violated invariant.
fn run(steps: &[Step], v: Variant) -> Result<World, String> {
    let mut w = World::initial();
    for &step in steps {
        apply(&mut w, step, v)?;
    }
    Ok(w)
}

/// n! / (k1! k2! ...): the number of interleavings of sequences of these lengths.
fn multinomial(lengths: &[usize]) -> u128 {
    let mut result = 1_u128;
    let mut placed = 0_u128;
    for &k in lengths {
        for j in 1..=k as u128 {
            placed += 1;
            result = result * placed / j;
        }
    }
    result
}

/// The previous model's restore: no retry, no ACTIVE flip.
const RESTORE_0: &[Step] = &[
    Step::RClaim(0, true),
    Step::RInsertTarget(0),
    Step::RRelease(0),
    Step::RCopyRead(0),
    Step::RHandOver(0),
];
const RESTORE_A: &[Step] = &[
    Step::RClaim(0, true),
    Step::RInsertTarget(0),
    Step::RRelease(0),
    Step::RCopyRead(0),
    Step::RHandOver(0),
    Step::RActivate(0),
];
const RESTORE_B: &[Step] = &[
    Step::RClaim(1, false),
    Step::RClaim(1, true),
    Step::RInsertTarget(1),
    Step::RRelease(1),
    Step::RCopyRead(1),
    Step::RHandOver(1),
    Step::RActivate(1),
];
const DELETE: &[Step] = &[
    Step::DCheckTarget,
    Step::DClaim,
    Step::DRecheckTarget,
    Step::DDropData,
    Step::DMarkDeleted,
];
const SWEEP_DELETE: &[Step] = &[
    Step::STakeOverDelete,
    Step::SCheckTarget,
    Step::SDropData,
    Step::SMarkDeleted,
];
/// The previous model's sweep event sets.
const OLD_SWEEPS: &[&[Step]] = &[
    &[],
    &[Step::SClearRestoreClaim],
    &[Step::SRemoveTarget(0)],
    &[Step::SClearRestoreClaim, Step::SRemoveTarget(0)],
    &[Step::SRemoveTarget(0), Step::SClearRestoreClaim],
];

/// Every thread: two restores, DeleteBackup and its stop, four sweep events,
/// the delete takeover and its stop.
fn full_model() -> Vec<&'static [Step]> {
    vec![
        RESTORE_A,
        RESTORE_B,
        DELETE,
        &[Step::DStop],
        &[Step::SClearRestoreClaim],
        &[Step::SClearRestoreClaim],
        &[Step::SRemoveTarget(0)],
        &[Step::SRemoveTarget(1)],
        SWEEP_DELETE,
        &[Step::SStop],
    ]
}

fn lengths(threads: &[&[Step]]) -> Vec<usize> {
    threads.iter().map(|t| t.len()).collect()
}

#[test]
fn the_original_20_orderings_hold_without_a_sweep() {
    // restore {claim, insert, release} x delete {claim, recheck, drop}.
    let orders = interleavings(&[
        &[
            Step::RClaim(0, true),
            Step::RInsertTarget(0),
            Step::RRelease(0),
        ],
        &[Step::DClaim, Step::DRecheckTarget, Step::DDropData],
    ]);
    assert_eq!(orders.len(), 20);
    for order in orders {
        run(&order, Variant::default()).unwrap_or_else(|e| panic!("{order:?}: {e}"));
    }
}

#[test]
fn the_explorer_agrees_with_brute_force_enumeration() {
    // The previous model's 39,060 interleavings, checked both ways, for the
    // fixed protocol and for a variant that the model must reject.
    for v in [
        Variant::default(),
        Variant {
            no_lost_claim_recheck: true,
            ..Variant::default()
        },
        Variant {
            release_after_drop: true,
            ..Variant::default()
        },
    ] {
        let mut total = 0_u128;
        for sweep in OLD_SWEEPS {
            let threads = [RESTORE_0, DELETE, sweep];
            let explored = explore(&threads, v);
            let orders = interleavings(&threads);
            assert_eq!(orders.len() as u128, multinomial(&lengths(&threads)));
            let brute_bad = orders.iter().filter(|o| run(o, v).is_err()).count();
            // The explorer stops a schedule at its first violation, so it
            // counts exactly the schedules that never violate an invariant.
            assert_eq!(
                explored.interleavings,
                (orders.len() - brute_bad) as u128,
                "{v:?} {sweep:?}"
            );
            assert_eq!(
                explored.violations.is_empty(),
                brute_bad == 0,
                "{v:?} {sweep:?}"
            );
            total += orders.len() as u128;
        }
        assert_eq!(total, 252 + 2 * 2772 + 2 * 16632);
    }
    // And with the DeleteBackup stop, where the B1 variant must fail both ways.
    let threads: [&[Step]; 3] = [RESTORE_0, DELETE, &[Step::DStop]];
    let orders = interleavings(&threads);
    let v = Variant {
        release_after_drop: true,
        ..Variant::default()
    };
    assert!(orders.iter().any(|o| run(o, v).is_err()));
    assert!(!explore(&threads, v).violations.is_empty());
    assert!(orders.iter().all(|o| run(o, Variant::default()).is_ok()));
}

#[test]
#[ignore = "explores the full model (about 25 s in a debug build); run with --include-ignored"]
fn no_interleaving_of_the_full_model_breaks_the_protocol() {
    let threads = full_model();
    let explored = explore(&threads, Variant::default());
    assert!(
        explored.violations.is_empty(),
        "{} violation(s), first: {}",
        explored.violations.len(),
        explored.violations[0]
    );
    // Every interleaving was covered: 28 steps in 10 threads.
    assert_eq!(explored.interleavings, multinomial(&lengths(&threads)));
    assert_eq!(explored.interleavings, 29_173_282_778_255_616_000);
    // Liveness: the protocol does make progress.
    let t = &explored.terminals;
    assert!(
        t.iter()
            .any(|w| !w.available && w.restores.iter().any(|r| r.target == Target::Active)),
        "no interleaving completes both a restore and the delete"
    );
    assert!(
        t.iter()
            .any(|w| w.restores.iter().all(|r| r.target == Target::Active)),
        "no interleaving completes both restores of one backup"
    );
    assert!(
        t.iter().any(|w| w.restores[1].target == Target::Active
            && w.restores[0].target == Target::Active
            && !w.available),
        "no interleaving completes both restores and then the delete"
    );
}

#[test]
fn without_stops_or_sweeps_no_claim_is_left_behind() {
    let threads: [&[Step]; 3] = [RESTORE_A, RESTORE_B, DELETE];
    let explored = explore(&threads, Variant::default());
    assert!(explored.violations.is_empty(), "{:?}", explored.violations);
    assert_eq!(explored.interleavings, multinomial(&[6, 7, 5]));
    for w in &explored.terminals {
        assert_eq!(w.restore_claim, None, "{w:?}");
        assert_eq!(w.delete_claim, None, "{w:?}");
        // Without a stall, a restore that claimed always completes.
        for r in &w.restores {
            assert!(!r.claimed || r.target == Target::Active, "{w:?}");
        }
    }
    // The retry lets the second restore claim after the first released in
    // some interleaving where its first attempt failed; without the retry
    // step that schedule ends the restore.
    assert!(
        explored
            .terminals
            .iter()
            .any(|w| w.restores.iter().all(|r| r.target == Target::Active))
    );
}

#[test]
#[ignore = "explores the full model (about 25 s in a debug build); run with --include-ignored"]
fn a_delete_claim_released_after_the_drop_is_found() {
    // B1: the claim released by a stop after the drop.
    let explored = explore(
        &full_model(),
        Variant {
            release_after_drop: true,
            ..Variant::default()
        },
    );
    assert!(!explored.violations.is_empty());
    assert!(
        explored.violations.iter().any(|e| e
            .contains("AVAILABLE and unclaimed after its data was dropped")
            && e.contains("DDropData, DStop")),
        "{}",
        explored.violations[0]
    );
}

#[test]
#[ignore = "explores the full model (about 25 s in a debug build); run with --include-ignored"]
fn a_sweep_that_releases_a_taken_over_delete_claim_is_found() {
    // The sweep before this fix: the stalled DeleteBackup it took over from
    // can still drop the data after the sweep released the claim.
    let explored = explore(
        &full_model(),
        Variant {
            sweep_releases: true,
            ..Variant::default()
        },
    );
    assert!(!explored.violations.is_empty());
    assert!(
        explored.violations.iter().all(|e| e
            .contains("AVAILABLE and unclaimed after its data was dropped")
            || e.contains("dropped backup data")),
        "{}",
        explored.violations[0]
    );
}

#[test]
#[ignore = "explores the full model (about 25 s in a debug build); run with --include-ignored"]
fn the_lost_claim_recheck_is_what_keeps_a_stalled_restore_safe() {
    let explored = explore(
        &full_model(),
        Variant {
            no_lost_claim_recheck: true,
            ..Variant::default()
        },
    );
    assert!(!explored.violations.is_empty());
    assert!(
        explored
            .violations
            .iter()
            .any(|e| e.contains("dropped backup data")),
        "{}",
        explored.violations[0]
    );
    // Without a sweep (no lapse) the recheck never matters.
    let threads: [&[Step]; 3] = [RESTORE_A, RESTORE_B, DELETE];
    assert!(
        explore(
            &threads,
            Variant {
                no_lost_claim_recheck: true,
                ..Variant::default()
            }
        )
        .violations
        .is_empty()
    );
}
