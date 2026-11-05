// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! An exhaustive model of the legacy restore / DeleteBackup protocol, with
//! the reconciler as an adversary. It is a model of the protocol, not a test
//! of the code: each step stands for one MongoDB operation, or one
//! transaction, in `register_restore`, `copy_restore`, `delete` or the
//! reconciler's removal of a target, and the model is kept in step with them
//! by hand.
//!
//! Threads, each a fixed sequence of steps; every interleaving of them is
//! checked:
//!
//! - two restores of the same backup into different targets: register (one
//!   transaction: the backup must be AVAILABLE and no CREATING target of it
//!   may exist, else the restore is refused), read the backup's data (`$out`,
//!   as one read), hand the target over (the job record DONE), and the
//!   backfill worker flipping it ACTIVE;
//! - DeleteBackup: the unlocked target check, the decision (one transaction:
//!   AVAILABLE and no CREATING target, then DELETING), the drop of the data,
//!   DELETED;
//! - DeleteBackup stopping at any point (its request future dropped, or an
//!   error returned, including an ambiguous error from the drop itself):
//!   nothing is reverted;
//! - a retry of DeleteBackup, which resumes a DELETING backup: drops the data
//!   again (idempotent) and writes DELETED;
//! - the reconciler removing each CREATING target that was not handed over,
//!   as it does once the owner's heartbeat is stale; the step may fire at any
//!   point, whether or not the owner is stalled, which over-approximates what
//!   the reconciler can do.
//!
//! Not modelled: `$out` as a non-atomic read, more than one restore of each
//! target, the timing of heartbeats (staleness is assumed possible at every
//! step), a failed write, and a registration's transaction retry (a write
//! conflict re-runs the registration, which the model's atomic step stands
//! for).
//!
//! Invariants, checked after every step: no restore is handed over after
//! reading dropped data; the backup is never AVAILABLE after its data was
//! dropped (a later restore would copy nothing); a DELETING backup never
//! reads AVAILABLE again. Liveness: some interleaving completes a restore and
//! the delete, some completes both restores, and without a stop no
//! interleaving leaves the backup DELETING.
//!
//! The checker explores reachable states once each (depth first, memoised on
//! the state and every thread's position) and counts the interleavings below
//! each state, so the count is the number of interleavings covered without
//! enumerating them. A brute-force enumerator cross-checks it on a smaller
//! configuration.

use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    // Restore `i` into target `i`.
    RRegister(usize),
    RCopyRead(usize),
    RHandOver(usize),
    RActivate(usize),
    // DeleteBackup
    DCheckTarget,
    DDecide,
    DDropData,
    DMarkDeleted,
    /// The DeleteBackup request stops: future dropped, or an error returned.
    DStop,
    /// A retry of DeleteBackup finding the backup DELETING.
    DResume,
    /// The reconciler removes restore `i`'s target, judging its owner dead.
    SRemoveTarget(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Target {
    None,
    Creating,
    /// CREATING with `restore_backfill_pending` and the job record DONE:
    /// still in use, no longer the reconciler's.
    HandedOver,
    Active,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Status {
    Available,
    Deleting,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Restore {
    target: Target,
    copy_saw_data: Option<bool>,
    done: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct World {
    status: Status,
    data_present: bool,
    restores: [Restore; 2],
    d_done: bool,
    /// Whether the DeleteBackup request has written DELETING.
    d_decided: bool,
}

impl World {
    fn initial() -> Self {
        let restore = Restore {
            target: Target::None,
            copy_saw_data: None,
            done: false,
        };
        Self {
            status: Status::Available,
            data_present: true,
            restores: [restore; 2],
            d_done: false,
            d_decided: false,
        }
    }

    /// `restoring_target_filter`: any CREATING target, handed over or not.
    fn restoring_target(&self) -> bool {
        self.restores
            .iter()
            .any(|r| matches!(r.target, Target::Creating | Target::HandedOver))
    }
}

/// Earlier or weaker shapes of the protocol, to show the model notices each.
#[derive(Clone, Copy, Debug, Default)]
struct Variant {
    /// The delete's decision without the in-use check under the lock: only
    /// the unlocked first check guards a registered target.
    decide_without_target_check: bool,
    /// A registration that does not require the backup to be AVAILABLE.
    register_without_status_check: bool,
    /// A DeleteBackup that stops after its decision puts the backup back to
    /// AVAILABLE (the shape of a claim released on error).
    revert_on_stop: bool,
}

fn apply(w: &mut World, step: Step, v: Variant) -> Result<(), String> {
    match step {
        Step::RRegister(i) => {
            if !w.restores[i].done {
                let available = w.status == Status::Available || v.register_without_status_check;
                if available && !w.restoring_target() {
                    w.restores[i].target = Target::Creating;
                } else {
                    w.restores[i].done = true;
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
        Step::DDecide => {
            if !w.d_done {
                let in_use = !v.decide_without_target_check && w.restoring_target();
                if w.status == Status::Available && !in_use {
                    w.status = Status::Deleting;
                    w.d_decided = true;
                } else {
                    // In use: the request ends here. Not AVAILABLE (a
                    // concurrent delete decided first): the code resumes that
                    // delete's drop, which the model gives its own step,
                    // `DResume`; this request's own steps end here.
                    w.d_done = true;
                }
            }
        }
        Step::DDropData => {
            if !w.d_done {
                w.data_present = false;
            }
        }
        Step::DMarkDeleted => {
            if !w.d_done {
                if w.status == Status::Deleting {
                    w.status = Status::Deleted;
                }
                w.d_done = true;
            }
        }
        Step::DStop => {
            if !w.d_done {
                w.d_done = true;
                if v.revert_on_stop && w.d_decided && w.status == Status::Deleting {
                    w.status = Status::Available;
                }
            }
        }
        Step::DResume => {
            if w.status == Status::Deleting {
                w.data_present = false;
                w.status = Status::Deleted;
            }
        }
        Step::SRemoveTarget(i) => {
            if w.restores[i].target == Target::Creating {
                w.restores[i].target = Target::None;
            }
        }
    }
    if w.status == Status::Available && !w.data_present {
        return Err(format!(
            "backup AVAILABLE after its data was dropped: {w:?}"
        ));
    }
    if w.d_decided && w.status == Status::Available {
        return Err(format!("a DELETING backup read AVAILABLE again: {w:?}"));
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

const RESTORE_A: &[Step] = &[
    Step::RRegister(0),
    Step::RCopyRead(0),
    Step::RHandOver(0),
    Step::RActivate(0),
];
const RESTORE_B: &[Step] = &[
    Step::RRegister(1),
    Step::RCopyRead(1),
    Step::RHandOver(1),
    Step::RActivate(1),
];
const DELETE: &[Step] = &[
    Step::DCheckTarget,
    Step::DDecide,
    Step::DDropData,
    Step::DMarkDeleted,
];

/// Every thread: two restores, DeleteBackup, its stop, a retry, and the
/// reconciler's removal of each target.
fn full_model() -> Vec<&'static [Step]> {
    vec![
        RESTORE_A,
        RESTORE_B,
        DELETE,
        &[Step::DStop],
        &[Step::DResume],
        &[Step::SRemoveTarget(0)],
        &[Step::SRemoveTarget(1)],
    ]
}

fn lengths(threads: &[&[Step]]) -> Vec<usize> {
    threads.iter().map(|t| t.len()).collect()
}

/// A restore's registration and DeleteBackup's decision, as the two
/// transactions order: every interleaving of one restore and one delete,
/// without a stop or the reconciler, ends with the delete DELETED and the
/// restore refused, the restore ACTIVE and the delete refused, or the delete
/// DELETED after the restore reached ACTIVE; never a delete that proceeded
/// while the target was CREATING.
#[test]
fn a_registration_and_a_delete_order_wholly_one_before_the_other() {
    let threads: [&[Step]; 2] = [RESTORE_A, DELETE];
    let orders = interleavings(&threads);
    assert_eq!(orders.len() as u128, multinomial(&[4, 4]));
    let mut delete_won = 0;
    let mut restore_won = 0;
    let mut both = 0;
    for order in &orders {
        let w = run(order, Variant::default()).unwrap_or_else(|e| panic!("{order:?}: {e}"));
        match (w.status, w.restores[0].target) {
            (Status::Deleted, Target::None) => delete_won += 1,
            (Status::Available, Target::Active) => restore_won += 1,
            (Status::Deleted, Target::Active) => both += 1,
            other => panic!("{order:?} ended in {other:?}"),
        }
    }
    assert!(
        delete_won > 0 && restore_won > 0 && both > 0,
        "{delete_won} / {restore_won} / {both}"
    );
}

#[test]
fn the_explorer_agrees_with_brute_force_enumeration() {
    for v in [
        Variant::default(),
        Variant {
            decide_without_target_check: true,
            ..Variant::default()
        },
        Variant {
            revert_on_stop: true,
            ..Variant::default()
        },
    ] {
        let threads: [&[Step]; 4] = [RESTORE_A, DELETE, &[Step::DStop], &[Step::SRemoveTarget(0)]];
        let explored = explore(&threads, v);
        let orders = interleavings(&threads);
        assert_eq!(orders.len() as u128, multinomial(&lengths(&threads)));
        let brute_bad = orders.iter().filter(|o| run(o, v).is_err()).count();
        // The explorer stops a schedule at its first violation, so it counts
        // exactly the schedules that never violate an invariant.
        assert_eq!(
            explored.interleavings,
            (orders.len() - brute_bad) as u128,
            "{v:?}"
        );
        assert_eq!(explored.violations.is_empty(), brute_bad == 0, "{v:?}");
    }
}

#[test]
fn no_interleaving_of_the_full_model_breaks_the_protocol() {
    let threads = full_model();
    let explored = explore(&threads, Variant::default());
    assert!(
        explored.violations.is_empty(),
        "{} violation(s), first: {}",
        explored.violations.len(),
        explored.violations[0]
    );
    // Every interleaving was covered: 16 steps in 7 threads.
    assert_eq!(explored.interleavings, multinomial(&lengths(&threads)));
    assert_eq!(explored.interleavings, 1_513_512_000);
    // Liveness: the protocol does make progress.
    let t = &explored.terminals;
    assert!(
        t.iter().any(|w| w.status == Status::Deleted
            && w.restores.iter().any(|r| r.target == Target::Active)),
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
            && w.status == Status::Deleted),
        "no interleaving completes both restores and then the delete"
    );
    // A DeleteBackup that stopped after writing DELETING leaves the backup
    // DELETING only until a retry resumes it; with the retry in the model, a
    // terminal state is DELETING only when the stop came after the retry.
    assert!(t.iter().any(|w| w.status == Status::Deleting));
}

#[test]
fn without_a_stop_the_backup_is_never_left_deleting() {
    let threads: [&[Step]; 5] = [
        RESTORE_A,
        RESTORE_B,
        DELETE,
        &[Step::SRemoveTarget(0)],
        &[Step::SRemoveTarget(1)],
    ];
    let explored = explore(&threads, Variant::default());
    assert!(explored.violations.is_empty(), "{:?}", explored.violations);
    assert_eq!(explored.interleavings, multinomial(&[4, 4, 4, 1, 1]));
    for w in &explored.terminals {
        assert_ne!(w.status, Status::Deleting, "{w:?}");
        // A delete that decided finished; one that did not left the backup
        // AVAILABLE with its data.
        assert_eq!(w.data_present, w.status == Status::Available, "{w:?}");
    }
}

#[test]
fn a_delete_deciding_without_the_in_use_check_under_the_lock_is_found() {
    let explored = explore(
        &full_model(),
        Variant {
            decide_without_target_check: true,
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
}

#[test]
fn a_registration_without_the_status_check_is_found() {
    let explored = explore(
        &full_model(),
        Variant {
            register_without_status_check: true,
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
}

#[test]
fn a_delete_that_reverts_on_a_stop_is_found() {
    let explored = explore(
        &full_model(),
        Variant {
            revert_on_stop: true,
            ..Variant::default()
        },
    );
    assert!(!explored.violations.is_empty());
    assert!(
        explored
            .violations
            .iter()
            .any(|e| e.contains("AVAILABLE after its data was dropped")
                && e.contains("DDropData, DStop")),
        "{}",
        explored.violations[0]
    );
}
