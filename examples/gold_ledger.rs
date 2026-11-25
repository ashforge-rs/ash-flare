//! A gold-coin ledger that loses no coins while its processes are killed at
//! random.
//!
//! ```text
//! treasury (one_for_one)
//! ├── journal       the only writer of the ledger; Shutdown::Infinity
//! ├── vault-0 … 7   one process per vault, rebuilt from the ledger on restart
//! └── courier-N     dynamic, Transient: one saga per transfer
//! chaos             kills a random journal, vault or courier every few ms
//! ```
//!
//! The ledger is an append-only list of records that outlives every process,
//! standing in for a file. Three rules keep the coins safe while anything may
//! be killed at any await point:
//!
//! - **Write before acting.** A vault records each decision in the ledger
//!   before it changes its balance or answers. A restarted vault replays the
//!   ledger and is exactly where its predecessor left off.
//! - **Every step is idempotent.** Each step of a transfer has an id. Asking
//!   again after a crash (`CallError::NoReply` or `Timeout`) returns the
//!   recorded decision instead of applying it twice, and the journal ignores a
//!   record it already holds.
//! - **Sagas resume.** A courier withdraws from one vault, travels, and
//!   deposits into another; if the destination is full it refunds the source.
//!   A courier killed half-way is restarted (`Transient`) and replays its steps,
//!   getting the recorded answers back until it reaches the step it had not
//!   done.
//!
//! Three phases:
//!
//! 1. Transfers run while chaos kills a process every 10ms.
//! 2. Kills come faster than the restart intensity allows; the treasury gives
//!    up, leaving transfers half-done.
//! 3. A new treasury opens on the same ledger and resumes every unfinished
//!    transfer, as a parent supervisor restarting it would. (ash-flare does
//!    not hand out handles to nested supervisors, so `main` plays the parent.)
//!
//! After each phase the ledger is audited: every coin must be in a vault or in
//! transit, every finished transfer must have exactly the records its outcome
//! needs, and the running vaults must agree with the ledger. The example exits
//! with an error if any check fails.
//!
//! Run with: `cargo run --example gold_ledger` (set `CHAOS_SEED` for a
//! different run).

use ash_flare::{
    CallError, ChildSpec, Mailbox, MailboxConfig, MailboxHandle, Registration, Registry, Reply,
    RestartBackoff, RestartIntensity, RestartPolicy, Shutdown, SupervisorEvent, SupervisorHandle,
    SupervisorSpec, Worker, mailbox_named,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const VAULTS: usize = 8;
const COINS_PER_VAULT: i64 = 1_000;
const TOTAL_COINS: i64 = COINS_PER_VAULT * VAULTS as i64;
const VAULT_CAPACITY: i64 = 2_000;
const TRANSFERS_PER_PHASE: u32 = 300;

const CALL_TIMEOUT: Duration = Duration::from_millis(250);
const RETRY_DELAY: Duration = Duration::from_millis(10);
const TRAVEL_TIME: Duration = Duration::from_millis(20);

// ============================================================================
// The ledger
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    Withdraw,
    Deposit,
    Refund,
}

/// One step of one transfer: the unit of idempotency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Op {
    transfer: u32,
    step: Step,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Applied,
    Refused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// The coins reached the destination.
    Delivered,
    /// The destination was full; the coins went back to the source.
    Returned,
    /// The source did not hold enough coins.
    Declined,
}

#[derive(Clone, Copy, Debug)]
struct Transfer {
    id: u32,
    from: usize,
    to: usize,
    coins: i64,
}

#[derive(Clone, Debug)]
enum Record {
    Mint {
        vault: usize,
        coins: i64,
    },
    Begin(Transfer),
    Decided {
        vault: usize,
        op: Op,
        decision: Decision,
        delta: i64,
    },
    End {
        transfer: u32,
        outcome: Outcome,
    },
}

/// What makes a record unique; the journal never stores two with one key.
#[derive(Debug, PartialEq, Eq, Hash)]
enum Key {
    Mint(usize),
    Begin(u32),
    Decided(usize, Op),
    End(u32),
}

impl Record {
    fn key(&self) -> Key {
        match self {
            Record::Mint { vault, .. } => Key::Mint(*vault),
            Record::Begin(transfer) => Key::Begin(transfer.id),
            Record::Decided { vault, op, .. } => Key::Decided(*vault, *op),
            Record::End { transfer, .. } => Key::End(*transfer),
        }
    }
}

/// Stands in for a file: it outlives every process, and the treasury itself.
type Disk = Arc<Mutex<Vec<Record>>>;

fn read(disk: &Disk) -> Vec<Record> {
    disk.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

// ============================================================================
// journal: the single writer of the ledger
// ============================================================================

enum JournalMsg {
    /// Stores a record unless one with its key is already stored.
    Append { record: Record, reply: Reply<()> },
    /// A vault's balance and decisions, rebuilt from the ledger.
    Replay {
        vault: usize,
        reply: Reply<VaultState>,
    },
}

type JournalRef = MailboxHandle<JournalMsg>;

struct Journal {
    inbox: Mailbox<JournalMsg>,
    disk: Disk,
    index: HashSet<Key>,
}

impl Worker for Journal {
    type Error = String;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        self.index = read(&self.disk).iter().map(Record::key).collect();
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(message) = self.inbox.recv().await {
            match message {
                JournalMsg::Append { record, reply } => {
                    // No await between writing and indexing: a kill lands
                    // either before both or after both.
                    if self.index.insert(record.key()) {
                        self.disk
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .push(record);
                    }
                    reply.send(());
                }
                JournalMsg::Replay { vault, reply } => {
                    reply.send(VaultState::replay(&read(&self.disk), vault));
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        let records = self
            .disk
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        println!("   [journal] closed cleanly, {records} records in the ledger");
        Ok(())
    }
}

/// Calls until an answer arrives. A timeout or a dropped reply means the
/// callee crashed mid-request; every request here is idempotent, so asking
/// again is safe.
async fn call_retrying<M, R>(
    to: &MailboxHandle<M>,
    request: impl Fn(Reply<R>) -> M,
) -> Result<R, String> {
    loop {
        match to.call(&request, CALL_TIMEOUT).await {
            Ok(answer) => return Ok(answer),
            Err(CallError::Closed(who)) => return Err(format!("{who} is gone")),
            Err(_) => sleep(RETRY_DELAY).await,
        }
    }
}

// ============================================================================
// vault: holds the gold of one vault
// ============================================================================

#[derive(Debug, Default)]
struct VaultState {
    balance: i64,
    decided: HashMap<Op, Decision>,
}

impl VaultState {
    fn replay(records: &[Record], vault: usize) -> Self {
        let mut state = Self::default();
        for record in records {
            match record {
                Record::Mint { vault: v, coins } if *v == vault => state.balance += coins,
                Record::Decided {
                    vault: v,
                    op,
                    decision,
                    delta,
                } if *v == vault => state.record(*op, *decision, *delta),
                _ => {}
            }
        }
        state
    }

    fn record(&mut self, op: Op, decision: Decision, delta: i64) {
        if decision == Decision::Applied {
            self.balance += delta;
        }
        self.decided.insert(op, decision);
    }

    fn decide(&self, op: Op, delta: i64) -> Decision {
        let after = self.balance + delta;
        // A refund is always taken back: the coins were here before.
        let fits = op.step == Step::Refund || (0..=VAULT_CAPACITY).contains(&after);
        if fits {
            Decision::Applied
        } else {
            Decision::Refused
        }
    }
}

enum VaultMsg {
    Apply {
        op: Op,
        delta: i64,
        reply: Reply<Decision>,
    },
    Balance(Reply<i64>),
}

type VaultRef = MailboxHandle<VaultMsg>;

fn vault_name(vault: usize) -> String {
    format!("vault-{vault}")
}

struct Vault {
    id: usize,
    inbox: Mailbox<VaultMsg>,
    handle: VaultRef,
    journal: JournalRef,
    registry: Registry,
    name: Option<Registration>,
    state: VaultState,
}

impl Worker for Vault {
    type Error = String;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        let vault = self.id;
        self.state =
            call_retrying(&self.journal, |reply| JournalMsg::Replay { vault, reply }).await?;
        self.name = Some(
            self.registry
                .register(vault_name(self.id), self.handle.clone()),
        );
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(message) = self.inbox.recv().await {
            match message {
                VaultMsg::Balance(reply) => reply.send(self.state.balance),
                VaultMsg::Apply { op, delta, reply } => {
                    if let Some(&decision) = self.state.decided.get(&op) {
                        reply.send(decision); // asked again after a crash
                        continue;
                    }
                    let decision = self.state.decide(op, delta);
                    let record = Record::Decided {
                        vault: self.id,
                        op,
                        decision,
                        delta,
                    };
                    call_retrying(&self.journal, |reply| JournalMsg::Append {
                        record: record.clone(),
                        reply,
                    })
                    .await?;
                    self.state.record(op, decision, delta);
                    reply.send(decision);
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// courier: one saga per transfer
// ============================================================================

struct Courier {
    transfer: Transfer,
    journal: JournalRef,
    registry: Registry,
}

impl Courier {
    /// Applies one step at a vault, found by name, until it answers.
    async fn apply(&self, vault: usize, step: Step, delta: i64) -> Decision {
        let op = Op {
            transfer: self.transfer.id,
            step,
        };
        loop {
            // Not registered while the vault is between two instances.
            if let Some(handle) = self.registry.whereis::<VaultRef>(&vault_name(vault)) {
                let request = |reply| VaultMsg::Apply { op, delta, reply };
                if let Ok(decision) = handle.call(request, CALL_TIMEOUT).await {
                    return decision;
                }
            }
            sleep(RETRY_DELAY).await;
        }
    }

    async fn append(&self, record: Record) -> Result<(), String> {
        call_retrying(&self.journal, |reply| JournalMsg::Append {
            record: record.clone(),
            reply,
        })
        .await
    }
}

impl Worker for Courier {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let Transfer {
            id,
            from,
            to,
            coins,
        } = self.transfer;
        self.append(Record::Begin(self.transfer)).await?;

        let outcome = match self.apply(from, Step::Withdraw, -coins).await {
            Decision::Refused => Outcome::Declined,
            Decision::Applied => {
                sleep(TRAVEL_TIME).await;
                match self.apply(to, Step::Deposit, coins).await {
                    Decision::Applied => Outcome::Delivered,
                    Decision::Refused => {
                        self.apply(from, Step::Refund, coins).await;
                        Outcome::Returned
                    }
                }
            }
        };

        self.append(Record::End {
            transfer: id,
            outcome,
        })
        .await
    }
}

// ============================================================================
// The treasury
// ============================================================================

struct Treasury {
    handle: SupervisorHandle,
    journal: JournalRef,
    registry: Registry,
    restarts: Arc<AtomicU64>,
    gave_up: Arc<AtomicBool>,
}

impl Treasury {
    /// Opens a treasury on the ledger in `disk`.
    fn open(disk: &Disk, registry: &Registry) -> Result<Self, Box<dyn std::error::Error>> {
        let (journal, journal_inbox) =
            mailbox_named::<JournalMsg>(MailboxConfig::unbounded(), "journal");
        let journal_disk = Arc::clone(disk);
        let mut spec = SupervisorSpec::new("treasury")
            // Chaos kills at most 100 processes a second; allow three times
            // that, but not the meltdown of phase 2.
            .with_restart_intensity(RestartIntensity::new(300, 1))
            .with_restart_backoff(RestartBackoff::exponential(
                Duration::from_millis(5),
                Duration::from_millis(50),
            ))
            .with_child(
                ChildSpec::new("journal", move || Journal {
                    inbox: journal_inbox.clone(),
                    disk: Arc::clone(&journal_disk),
                    index: HashSet::new(),
                })
                // Never cut short when the treasury stops.
                .shutdown(Shutdown::Infinity),
            );

        for id in 0..VAULTS {
            let (handle, inbox) =
                mailbox_named::<VaultMsg>(MailboxConfig::default(), vault_name(id));
            let (journal, registry) = (journal.clone(), registry.clone());
            spec = spec.with_worker(
                vault_name(id),
                move || Vault {
                    id,
                    inbox: inbox.clone(),
                    handle: handle.clone(),
                    journal: journal.clone(),
                    registry: registry.clone(),
                    name: None,
                    state: VaultState::default(),
                },
                RestartPolicy::Permanent,
            );
        }

        let handle = SupervisorHandle::start(spec)?;
        let (restarts, gave_up) = (
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicBool::new(false)),
        );
        let mut events = handle.subscribe();
        let (counted, flagged) = (Arc::clone(&restarts), Arc::clone(&gave_up));
        tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match events.recv().await {
                    Ok(SupervisorEvent::ChildRestarted { .. }) => {
                        counted.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(SupervisorEvent::RestartIntensityExceeded) => {
                        flagged.store(true, Ordering::Relaxed);
                    }
                    Ok(SupervisorEvent::Stopped { .. }) | Err(RecvError::Closed) => break,
                    Ok(_) | Err(RecvError::Lagged(_)) => {}
                }
            }
        });

        Ok(Self {
            handle,
            journal,
            registry: registry.clone(),
            restarts,
            gave_up,
        })
    }

    /// Starts a courier for `transfer`.
    async fn dispatch(&self, transfer: Transfer) -> Result<(), Box<dyn std::error::Error>> {
        let (journal, registry) = (self.journal.clone(), self.registry.clone());
        self.handle
            .start_child(
                format!("courier-{}", transfer.id),
                move || Courier {
                    transfer,
                    journal: journal.clone(),
                    registry: registry.clone(),
                },
                // Restarted when killed, done once it has finished.
                RestartPolicy::Transient,
            )
            .await?;
        Ok(())
    }

    /// Every vault's balance, as the running vaults report it.
    async fn live_balances(&self) -> Result<Vec<i64>, String> {
        let mut balances = Vec::with_capacity(VAULTS);
        for vault in 0..VAULTS {
            let handle = loop {
                match self.registry.whereis::<VaultRef>(&vault_name(vault)) {
                    Some(handle) => break handle,
                    None => sleep(RETRY_DELAY).await,
                }
            };
            balances.push(call_retrying(&handle, VaultMsg::Balance).await?);
        }
        Ok(balances)
    }
}

// ============================================================================
// Chaos
// ============================================================================

/// A small deterministic random number generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % bound
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            return None;
        }
        items.get(self.below(items.len() as u64) as usize)
    }
}

#[derive(Debug, Default)]
struct Kills {
    journal: u32,
    vaults: u32,
    couriers: u32,
}

/// Kills a random child every `every` until `stop` is set.
async fn chaos(
    handle: SupervisorHandle,
    mut rng: Rng,
    every: Duration,
    stop: Arc<AtomicBool>,
) -> Kills {
    let mut kills = Kills::default();
    while !stop.load(Ordering::Relaxed) {
        sleep(every).await;
        let Ok(children) = handle.which_children().await else {
            break;
        };
        let couriers: Vec<_> = children
            .iter()
            .map(|child| child.id.as_str())
            .filter(|id| id.starts_with("courier-"))
            .collect();
        let target = match rng.below(10) {
            0 => "journal".to_owned(),
            1..=3 => vault_name(rng.below(VAULTS as u64) as usize),
            _ => match rng.pick(&couriers) {
                Some(courier) => (*courier).to_owned(),
                None => continue,
            },
        };
        if let Ok(true) = handle.kill_child(&target).await {
            match target.as_str() {
                "journal" => kills.journal += 1,
                id if id.starts_with("vault-") => kills.vaults += 1,
                _ => kills.couriers += 1,
            }
        }
    }
    kills
}

/// Kills every child at once, over and over, until the supervisor gives up.
async fn meltdown(handle: SupervisorHandle) -> usize {
    let mut kills = 0;
    while let Ok(children) = handle.which_children().await {
        let mut wave = tokio::task::JoinSet::new();
        for child in children {
            let handle = handle.clone();
            wave.spawn(async move { handle.kill_child(&child.id).await });
        }
        kills += wave
            .join_all()
            .await
            .into_iter()
            .filter(|killed| matches!(killed, Ok(true)))
            .count();
    }
    kills
}

// ============================================================================
// The audit
// ============================================================================

struct Audit {
    balances: [i64; VAULTS],
    in_transit: i64,
    unfinished: Vec<u32>,
    outcomes: HashMap<u32, Outcome>,
    problems: Vec<String>,
}

impl Audit {
    fn of(disk: &Disk) -> Self {
        let records = read(disk);
        let mut balances = [0; VAULTS];
        let mut keys = HashSet::new();
        let mut begun = Vec::new();
        let mut decided: HashMap<Op, (Decision, i64)> = HashMap::new();
        let mut outcomes = HashMap::new();
        let mut problems = Vec::new();

        for record in &records {
            if !keys.insert(record.key()) {
                problems.push(format!("duplicate record {:?}", record.key()));
            }
            match record {
                Record::Mint { vault, coins } => balances[*vault] += coins,
                Record::Begin(transfer) => begun.push(transfer.id),
                Record::Decided {
                    vault,
                    op,
                    decision,
                    delta,
                } => {
                    if *decision == Decision::Applied {
                        balances[*vault] += delta;
                    }
                    decided.insert(*op, (*decision, *delta));
                }
                Record::End { transfer, outcome } => {
                    outcomes.insert(*transfer, *outcome);
                }
            }
        }

        let step = |transfer, step| decided.get(&Op { transfer, step }).map(|(d, _)| *d);
        for (&id, &outcome) in &outcomes {
            use Decision::{Applied, Refused};
            let steps = (
                step(id, Step::Withdraw),
                step(id, Step::Deposit),
                step(id, Step::Refund),
            );
            let expected = match outcome {
                Outcome::Delivered => (Some(Applied), Some(Applied), None),
                Outcome::Returned => (Some(Applied), Some(Refused), Some(Applied)),
                Outcome::Declined => (Some(Refused), None, None),
            };
            if steps != expected {
                problems.push(format!(
                    "transfer {id} is {outcome:?} but its steps are {steps:?}"
                ));
            }
        }

        // Coins withdrawn by unfinished transfers and not yet put back down.
        let unfinished: Vec<u32> = begun
            .into_iter()
            .filter(|id| !outcomes.contains_key(id))
            .collect();
        let decided = &decided;
        let in_transit = unfinished
            .iter()
            .flat_map(|&transfer| {
                [Step::Withdraw, Step::Deposit, Step::Refund]
                    .into_iter()
                    .filter_map(move |step| decided.get(&Op { transfer, step }))
            })
            .filter(|(decision, _)| *decision == Decision::Applied)
            .map(|(_, delta)| -delta)
            .sum();

        let in_vaults: i64 = balances.iter().sum();
        if in_vaults + in_transit != TOTAL_COINS {
            problems.push(format!(
                "{in_vaults} coins in vaults and {in_transit} in transit, but {TOTAL_COINS} were minted"
            ));
        }

        Self {
            balances,
            in_transit,
            unfinished,
            outcomes,
            problems,
        }
    }

    fn report(&self, ids: impl Iterator<Item = u32>) {
        let (mut delivered, mut returned, mut declined) = (0, 0, 0);
        for id in ids {
            match self.outcomes.get(&id) {
                Some(Outcome::Delivered) => delivered += 1,
                Some(Outcome::Returned) => returned += 1,
                Some(Outcome::Declined) => declined += 1,
                None => {}
            }
        }
        println!(
            "   transfers: {delivered} delivered, {returned} returned (destination full), \
             {declined} declined (not enough gold)"
        );
        let in_vaults: i64 = self.balances.iter().sum();
        println!(
            "   ledger:    {in_vaults} coins in vaults + {} in transit ({} unfinished transfers) \
             = {} of {TOTAL_COINS} minted  {}",
            self.in_transit,
            self.unfinished.len(),
            in_vaults + self.in_transit,
            mark(self.problems.is_empty()),
        );
        for problem in self.problems.iter().take(10) {
            println!("   PROBLEM:   {problem}");
        }
        if self.problems.len() > 10 {
            println!("   ... and {} more problems", self.problems.len() - 10);
        }
    }
}

fn mark(ok: bool) -> &'static str {
    if ok { "✓" } else { "✗" }
}

/// Waits until every transfer in `ids` has finished, according to the ledger.
async fn settle(disk: &Disk, ids: &[u32]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let finished: HashSet<u32> = read(disk)
            .iter()
            .filter_map(|record| match record {
                Record::End { transfer, .. } => Some(*transfer),
                _ => None,
            })
            .collect();
        if ids.iter().all(|id| finished.contains(id)) {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err("transfers did not finish within 60s".to_owned());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Compares the running vaults with the ledger.
async fn check_vaults(treasury: &Treasury, audit: &Audit) -> Result<bool, String> {
    let live = treasury.live_balances().await?;
    let ok = live == audit.balances;
    println!(
        "   vaults:    {live:?} {}",
        if ok {
            "match the ledger ✓"
        } else {
            "DO NOT match the ledger ✗"
        }
    );
    Ok(ok)
}

// ============================================================================
// main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let seed = std::env::var("CHAOS_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or(20_261_125);
    let mut rng = Rng(seed | 1);

    let minted = (0..VAULTS).map(|vault| Record::Mint {
        vault,
        coins: COINS_PER_VAULT,
    });
    let disk: Disk = Arc::new(Mutex::new(minted.collect()));
    let registry = Registry::new();

    let orders: Vec<Transfer> = (0..2 * TRANSFERS_PER_PHASE)
        .map(|id| {
            let from = rng.below(VAULTS as u64) as usize;
            let to = (from + 1 + rng.below(VAULTS as u64 - 1) as usize) % VAULTS;
            let coins = 50 + rng.below(400) as i64;
            Transfer {
                id,
                from,
                to,
                coins,
            }
        })
        .collect();
    let (first, second) = orders.split_at(TRANSFERS_PER_PHASE as usize);
    let ids = |orders: &[Transfer]| orders.iter().map(|t| t.id).collect::<Vec<_>>();
    let mut all_ok = true;

    println!("=== Gold ledger ===");
    println!(
        "{VAULTS} vaults of {COINS_PER_VAULT} gold coins ({TOTAL_COINS} in all), \
         at most {VAULT_CAPACITY} per vault; CHAOS_SEED={seed}"
    );

    // ------------------------------------------------------------------------
    println!(
        "\n-- phase 1: {TRANSFERS_PER_PHASE} transfers while chaos kills a process every 10ms"
    );
    let treasury = Treasury::open(&disk, &registry)?;
    let stop = Arc::new(AtomicBool::new(false));
    let chaos_task = tokio::spawn(chaos(
        treasury.handle.clone(),
        Rng(seed.rotate_left(17) | 1),
        Duration::from_millis(10),
        Arc::clone(&stop),
    ));
    for transfer in first {
        treasury.dispatch(*transfer).await?;
        sleep(Duration::from_millis(2)).await;
    }
    settle(&disk, &ids(first)).await?;
    stop.store(true, Ordering::Relaxed);
    let kills = chaos_task.await?;

    let audit = Audit::of(&disk);
    audit.report(first.iter().map(|t| t.id));
    println!(
        "   chaos:     killed the journal {}x, vaults {}x, couriers {}x; {} restarts",
        kills.journal,
        kills.vaults,
        kills.couriers,
        treasury.restarts.load(Ordering::Relaxed)
    );
    all_ok &= audit.problems.is_empty() && check_vaults(&treasury, &audit).await?;

    // ------------------------------------------------------------------------
    println!(
        "\n-- phase 2: {TRANSFERS_PER_PHASE} more transfers, then kills faster than restarts are allowed"
    );
    let (started, rest) = second.split_at(second.len() / 3);
    for transfer in started {
        treasury.dispatch(*transfer).await?;
    }
    // Kill everything while the rest are still being dispatched; dispatching
    // fails once the treasury has given up.
    let meltdown_task = tokio::spawn(meltdown(treasury.handle.clone()));
    let mut dispatched = started.len();
    for transfer in rest {
        if treasury.dispatch(*transfer).await.is_err() {
            break;
        }
        dispatched += 1;
    }
    let kills = tokio::time::timeout(Duration::from_secs(30), meltdown_task)
        .await
        .map_err(|_| "the treasury did not give up within 30s")??;
    while !treasury.handle.is_stopped() {
        sleep(RETRY_DELAY).await;
    }
    println!(
        "   dispatched {dispatched} of {} before the treasury went down",
        second.len()
    );
    println!(
        "   treasury:  {} after {kills} kills",
        if treasury.gave_up.load(Ordering::Relaxed) {
            "restart intensity exceeded, gave up"
        } else {
            "stopped"
        }
    );
    let audit = Audit::of(&disk);
    audit.report(second.iter().map(|t| t.id));
    all_ok &= audit.problems.is_empty();

    // ------------------------------------------------------------------------
    println!("\n-- recovery: a new treasury opens on the same ledger and resumes");
    let treasury = Treasury::open(&disk, &registry)?;
    // Half-done transfers first: their coins are in transit. Then the ones
    // that never began.
    let begun: HashSet<u32> = audit.unfinished.iter().copied().collect();
    let (resumed, fresh): (Vec<Transfer>, Vec<Transfer>) = second
        .iter()
        .filter(|t| !audit.outcomes.contains_key(&t.id))
        .partition(|t| begun.contains(&t.id));
    println!(
        "   resuming {} half-done transfers, then starting {} that never began",
        resumed.len(),
        fresh.len()
    );
    for transfer in &resumed {
        treasury.dispatch(*transfer).await?;
    }
    settle(&disk, &ids(&resumed)).await?;
    for transfer in &fresh {
        treasury.dispatch(*transfer).await?;
        sleep(Duration::from_millis(2)).await;
    }
    settle(&disk, &ids(second)).await?;

    let audit = Audit::of(&disk);
    audit.report(second.iter().map(|t| t.id));
    all_ok &= audit.problems.is_empty()
        && audit.in_transit == 0
        && check_vaults(&treasury, &audit).await?;

    treasury.handle.shutdown().await?;

    if all_ok {
        println!("\nAll checks passed: no gold coin was created or lost.");
        Ok(())
    } else {
        Err("the ledger does not balance".into())
    }
}
