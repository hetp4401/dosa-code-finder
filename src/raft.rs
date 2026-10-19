//! Minimal Raft for dosa's shared scan state.
//!
//! One replicated log holds the scan's life: Start, ChunksDone, Found, Stop, End. Every replica applies the
//! same entries in the same order, so every replica agrees on the one scan that runs, which chunks are done,
//! and what was found. Anything else (per-replica progress, chunk hand-outs) stays ephemeral outside the log.
//!
//! Deliberately small: fixed membership (1..=REPLICA_COUNT), no pre-vote, no joint consensus. A replica that
//! restarts (or joins late) catches up through InstallSnapshot + AppendEntries. Terms start at 1; log indices
//! are absolute and 1-based.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

use crate::coord::Node;

// ---- tuning ----

/// Election timeout: this + up to the same again, randomized per replica per term.
const ELECTION_BASE: Duration = Duration::from_millis(1500);
/// How often a leader heartbeats (empty AppendEntries).
const HEARTBEAT: Duration = Duration::from_millis(400);
/// The raft tick: timeouts, heartbeats, replication, applying.
pub const TICK: Duration = Duration::from_millis(50);
/// A client (HTTP) waits this long for its entry to commit.
const CLIENT_WAIT: Duration = Duration::from_secs(8);

/// A scan's shared state, as every replica knows it from the log.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScanState {
    pub id: String,
    pub from: u32,
    pub to: u32,
    pub seed: u64,
    pub state: String, // running | found | stopped | done
    pub done: Vec<bool>,
    pub code: String,
    pub message: String,
    pub started_at: u64,
}

impl ScanState {
    pub fn chunks(&self) -> usize {
        let total = self.to.saturating_sub(self.from) as usize;
        total.div_ceil(crate::coord::CHUNK as usize)
    }
    pub fn range(&self) -> String {
        format!("{:05}..{:05}", self.from, self.to)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Found {
    pub code: String,
    pub at: u64,
    pub found_at: String,
    pub range: String,
    pub replica: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Last {
    pub id: String,
    pub at: u64,
    pub from: u32,
    pub to: u32,
    pub status: String,
    pub code: String,
    pub by: u32,
}

/// Everything a slow replica needs: the whole state machine + where the log continues.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_index: u64,
    pub last_term: u64,
    pub scan: Option<ScanState>,
    pub history: Vec<Found>,
    pub last: Option<Last>,
}

/// The log's commands: the scan's life, nothing else.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "c", rename_all = "snake_case")]
pub enum Command {
    Start { id: String, from: u32, to: u32, seed: u64, at: u64 },
    ChunksDone { id: String, chunks: Vec<u32> },
    Found { id: String, code: String, by: u32, at: u64 },
    Stop { id: String, by: u32, at: u64 },
    End { id: String, at: u64 }, // every chunk answered, nothing found
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub cmd: Command,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// The wire: Raft RPCs plus the work plane (chunk hand-outs, telemetry) and client forwarding.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    Hello { id: u32, boot: String, term: u64, commit: u64, scan: Option<String> },
    RequestVote { term: u64, cand: u32, lli: u64, llt: u64 },
    RequestVoteReply { term: u64, granted: bool, by: u32 },
    AppendEntries { term: u64, leader: u32, pli: u64, plt: u64, entries: Vec<Entry>, lc: u64 },
    AppendEntriesReply { term: u64, ok: bool, idx: u64, by: u32 },
    InstallSnapshot { term: u64, leader: u32, snap: Snapshot },
    InstallSnapshotReply { term: u64, by: u32 },
    /// A client request (start/stop) forwarded to the leader.
    Forward { req: u64, from: u32, cmd: ForwardCmd },
    ForwardReply { req: u64, ok: bool, msg: String },
    /// Work plane (ephemeral, never logged): the leader hands a chunk to a worker.
    Assign { id: String, chunk: u32, codes: Vec<String> },
    /// A worker finished a chunk (broadcast; the leader batches these into the log).
    Done { id: String, chunk: u32, by: u32 },
    /// A worker found a valid code (broadcast; the leader logs it).
    FoundCode { id: String, code: String, by: u32 },
    /// Telemetry for the page's replica cards.
    Prog { id: String, by: u32, tried: u64, rate: f64, queue: usize, last_error: String, stats: crate::coord::Stats },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "c", rename_all = "snake_case")]
pub enum ForwardCmd {
    Start { from: u32, to: u32 },
    Stop,
    /// A found code from a worker: the leader logs it (fire-and-forget).
    Found { id: String, code: String },
}

pub struct Raft {
    pub term: u64,
    pub voted_for: Option<u32>,
    pub role: Role,
    pub leader: Option<u32>,
    /// Entries after base_index; entry i has index base_index + 1 + i.
    pub log: Vec<Entry>,
    pub base_index: u64,
    pub base_term: u64,
    pub commit: u64,
    pub applied: u64,
    next: HashMap<u32, u64>,
    matched: HashMap<u32, u64>,
    votes: HashSet<u32>,
    election_at: Instant,
    hb_at: Instant,
    /// The latest snapshot, kept for slow followers.
    pub snap: Option<Snapshot>,
    /// A Stop already appended but not yet applied (dedupes the fan-out's 24 forwards).
    stop_pending: Option<String>,
}

impl Raft {
    pub fn new() -> Raft {
        Raft {
            term: 1,
            voted_for: None,
            role: Role::Follower,
            leader: None,
            log: Vec::new(),
            base_index: 0,
            base_term: 0,
            commit: 0,
            applied: 0,
            next: HashMap::new(),
            matched: HashMap::new(),
            votes: HashSet::new(),
            election_at: Instant::now() + Self::election_timeout(),
            hb_at: Instant::now(),
            snap: None,
            stop_pending: None,
        }
    }

    fn election_timeout() -> Duration {
        ELECTION_BASE + Duration::from_millis(crate::coord::random_u64() % ELECTION_BASE.as_millis() as u64)
    }

    pub fn last_index(&self) -> u64 {
        self.base_index + self.log.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.base_term)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        if index == 0 || index <= self.base_index {
            return None;
        }
        self.log.get((index - self.base_index - 1) as usize)
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        if index == self.base_index {
            return self.base_term;
        }
        self.entry(index).map(|e| e.term).unwrap_or(0)
    }

    pub fn quorum(n: u32) -> usize {
        n as usize / 2 + 1
    }

    fn become_follower(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        self.election_at = Instant::now() + Self::election_timeout();
    }

    fn become_candidate(&mut self, me: u32) {
        self.term += 1;
        self.role = Role::Candidate;
        self.leader = None;
        self.voted_for = Some(me);
        self.votes.clear();
        self.votes.insert(me);
        self.election_at = Instant::now() + Self::election_timeout();
    }

    fn become_leader(&mut self, me: u32, n: u32) {
        self.role = Role::Leader;
        self.leader = Some(me);
        self.votes.clear();
        let last = self.last_index();
        self.next = (1..=n).filter(|r| *r != me).map(|r| (r, last + 1)).collect();
        self.matched = (1..=n).filter(|r| *r != me).map(|r| (r, 0)).collect();
        self.hb_at = Instant::now();
        println!("raft: replica {me} is leader for term {}", self.term);
    }
}

/// The replicated state machine: the scan, the codes found, the last scan.
pub struct StateMachine {
    pub scan: Option<ScanState>,
    pub history: Vec<Found>,
    pub last: Option<Last>,
}

impl StateMachine {
    pub fn new() -> StateMachine {
        StateMachine { scan: None, history: Vec::new(), last: None }
    }

    pub fn running(&self) -> bool {
        matches!(&self.scan, Some(s) if s.state == "running")
    }

    /// Apply one committed entry. Returns a side-effect for the node to run (worker start/stop).
    pub fn apply(&mut self, cmd: &Command) -> Applied {
        match cmd {
            Command::Start { id, from, to, seed, at } => {
                if self.running() {
                    return Applied::None;
                }
                let mut s = ScanState {
                    id: id.clone(), from: *from, to: *to, seed: *seed,
                    state: "running".into(), done: Vec::new(),
                    code: String::new(), message: String::new(), started_at: *at,
                };
                s.done = vec![false; s.chunks()];
                self.scan = Some(s);
                self.last = Some(Last { id: id.clone(), at: *at, from: *from, to: *to, status: "running".into(), code: String::new(), by: 0 });
                Applied::ScanStarted(id.clone())
            }
            Command::ChunksDone { id, chunks } => {
                let mut all = false;
                if let Some(s) = &mut self.scan {
                    if s.id == *id && s.state == "running" {
                        for c in chunks {
                            if (*c as usize) < s.done.len() {
                                s.done[*c as usize] = true;
                            }
                        }
                        all = s.done.iter().all(|d| *d);
                    }
                }
                if all {
                    self.finish_scan(id, "done", "", "scan complete, no code found");
                    Applied::ScanEnded(id.clone())
                } else {
                    Applied::None
                }
            }
            Command::Found { id, code, by, at } => {
                if let Some(s) = &mut self.scan {
                    if s.id == *id && s.state == "running" {
                        let range = s.range();
                        if !self.history.iter().any(|f| f.code == *code) {
                            self.history.push(Found { code: code.clone(), at: *at, found_at: crate::coord::utc_text(*at),
                                range, replica: *by });
                            let extra = self.history.len().saturating_sub(20);
                            self.history.drain(..extra);
                        }
                        let msg = format!("code {code} found by replica {by}");
                        println!("scan {id}: {msg}");
                        self.finish_scan(id, "found", code, &msg);
                        return Applied::ScanEnded(id.clone());
                    }
                }
                Applied::None
            }
            Command::Stop { id, by, at } => {
                if let Some(s) = &mut self.scan {
                    if s.id == *id && s.state == "running" {
                        let msg = format!("stopped from replica {by}");
                        println!("scan {id}: {msg}");
                        self.finish_scan(id, "stopped", "", &msg);
                        let _ = at;
                        return Applied::ScanEnded(id.clone());
                    }
                }
                Applied::None
            }
            Command::End { id, .. } => {
                if let Some(s) = &mut self.scan {
                    if s.id == *id && s.state == "running" {
                        self.finish_scan(id, "done", "", "scan complete, no code found");
                        return Applied::ScanEnded(id.clone());
                    }
                }
                Applied::None
            }
        }
    }

    fn finish_scan(&mut self, id: &str, status: &str, code: &str, message: &str) {
        if let Some(s) = &mut self.scan {
            if s.id == id {
                s.state = status.to_string();
                s.code = code.to_string();
                s.message = message.to_string();
            }
        }
        if let Some(l) = &mut self.last {
            if l.id == id {
                l.status = status.to_string();
                l.code = code.to_string();
            }
        }
    }

    pub fn snapshot(&self, last_index: u64, last_term: u64) -> Snapshot {
        Snapshot { last_index, last_term, scan: self.scan.clone(), history: self.history.clone(), last: self.last.clone() }
    }

    pub fn install(&mut self, snap: &Snapshot) {
        self.scan = snap.scan.clone();
        self.history = snap.history.clone();
        self.last = snap.last.clone();
    }
}

pub enum Applied {
    None,
    ScanStarted(String),
    ScanEnded(String),
}

// ---- the node-facing API (all take &Arc<Node>) ----

pub type FwdResult = (bool, String);

impl Node {
    pub fn send_to(&self, id: u32, m: &Msg) {
        if id == self.me {
            return;
        }
        if let Some(p) = self.peers.lock().unwrap().get(&id) {
            let _ = p.tx.send(serde_json::to_string(m).unwrap());
        }
    }

    pub fn broadcast(&self, m: &Msg) {
        let text = serde_json::to_string(m).unwrap();
        for p in self.peers.lock().unwrap().values() {
            let _ = p.tx.send(text.clone());
        }
    }

    fn raft_lock(&self) -> std::sync::MutexGuard<'_, Raft> {
        self.raft.lock().unwrap()
    }

    /// Incoming mesh message.
    pub fn on_msg(self: &Arc<Self>, from: u32, m: Msg) {
        match m {
            Msg::Hello { term, commit, .. } => self.on_hello(from, term, commit),
            Msg::RequestVote { term, cand, lli, llt } => self.on_request_vote(from, term, cand, lli, llt),
            Msg::RequestVoteReply { term, granted, by } => self.on_rv_reply(term, granted, by),
            Msg::AppendEntries { term, leader, pli, plt, entries, lc } => self.on_append(from, term, leader, pli, plt, entries, lc),
            Msg::AppendEntriesReply { term, ok, idx, by } => self.on_ae_reply(term, ok, idx, by),
            Msg::InstallSnapshot { term, leader, snap } => self.on_snapshot(term, leader, snap),
            Msg::InstallSnapshotReply { .. } => {}
            Msg::Forward { req, from, cmd } => self.on_forward(req, from, cmd),
            Msg::ForwardReply { req, ok, msg } => {
                if let Some(tx) = self.fwd.lock().unwrap().remove(&req) {
                    let _ = tx.send((ok, msg));
                }
            }
            Msg::Assign { id, chunk, codes } => self.take_codes(&id, chunk as usize, codes),
            Msg::Done { id, chunk, by } => self.note_done_msg(&id, chunk as usize, by),
            Msg::FoundCode { id, code, by } => self.on_found_code(&id, &code, by),
            Msg::Prog { id, by, tried, rate, queue, last_error, stats } => {
                self.progs.lock().unwrap().insert(by, crate::coord::PeerProg { scan: id, tried, rate, queue, last_error, stats, at: Instant::now() });
            }
        }
    }

    fn on_hello(self: &Arc<Self>, from: u32, term: u64, commit: u64) {
        let mut r = self.raft_lock();
        if term > r.term {
            r.become_follower(term);
        }
        // A peer far behind gets the snapshot (it just (re)started or joined).
        let need_snap = r.role == Role::Leader && r.snap.as_ref().map(|s| commit < s.last_index).unwrap_or(false);
        let snap = need_snap.then(|| r.snap.clone()).flatten();
        let term_now = r.term;
        drop(r);
        if let Some(s) = snap {
            println!("raft: sending snapshot to replica {from} (it is at {commit})");
            self.send_to(from, &Msg::InstallSnapshot { term: term_now, leader: self.me, snap: s });
        }
    }

    fn on_request_vote(self: &Arc<Self>, from: u32, term: u64, cand: u32, lli: u64, llt: u64) {
        let mut r = self.raft_lock();
        let mut granted = false;
        if term > r.term {
            r.become_follower(term);
        }
        if term == r.term && r.role != Role::Leader {
            let up_to_date = llt > r.last_term() || (llt == r.last_term() && lli >= r.last_index());
            if up_to_date && r.voted_for.map(|v| v == cand).unwrap_or(true) {
                r.voted_for = Some(cand);
                granted = true;
                r.election_at = Instant::now() + Raft::election_timeout();
            }
        }
        let t = r.term;
        drop(r);
        self.send_to(from, &Msg::RequestVoteReply { term: t, granted, by: self.me });
    }

    fn on_rv_reply(self: &Arc<Self>, term: u64, granted: bool, by: u32) {
        let mut r = self.raft_lock();
        if term > r.term {
            r.become_follower(term);
            return;
        }
        if term != r.term || r.role != Role::Candidate {
            return;
        }
        if granted {
            r.votes.insert(by);
            if r.votes.len() >= Raft::quorum(self.n) {
                let me = self.me;
                let n = self.n;
                r.become_leader(me, n);
                drop(r);
                self.replicate_all();
            }
        }
    }

    fn on_append(self: &Arc<Self>, from: u32, term: u64, leader: u32, pli: u64, plt: u64, entries: Vec<Entry>, lc: u64) {
        let mut r = self.raft_lock();
        if term > r.term {
            r.become_follower(term);
        }
        let mut ok = false;
        let mut idx = r.last_index();
        if term == r.term {
            r.leader = Some(leader);
            r.election_at = Instant::now() + Raft::election_timeout();
            if r.role == Role::Candidate {
                r.role = Role::Follower;
            }
            // Log consistency: our log must contain pli at term plt.
            let consistent = pli == 0 || (pli >= r.base_index && r.term_at(pli) == plt);
            if consistent {
                ok = true;
                // Drop what conflicts, append the rest.
                let mut i = 0;
                while i < entries.len() {
                    let e = &entries[i];
                    if e.index <= r.base_index {
                        i += 1;
                        continue;
                    }
                    match r.entry(e.index) {
                        Some(old) if old.term == e.term => i += 1,
                        _ => break,
                    }
                }
                // Truncate at the first divergence (or no-op if everything matched).
                let trunc_to = if i < entries.len() {
                    let base = r.base_index;
                    (entries[i].index - base - 1) as usize
                } else {
                    r.log.len()
                };
                let llen = r.log.len();
                r.log.truncate(trunc_to.min(llen));
                for e in entries.into_iter().skip(i) {
                    let want = r.base_index + r.log.len() as u64 + 1;
                    if e.index == want {
                        r.log.push(e);
                    } else if e.index > want {
                        break;
                    }
                }
                idx = r.last_index();
                if lc > r.commit {
                    r.commit = lc.min(r.last_index());
                }
            }
        }
        let t = r.term;
        let need_apply = r.commit > r.applied;
        drop(r);
        self.send_to(from, &Msg::AppendEntriesReply { term: t, ok, idx, by: self.me });
        if need_apply {
            self.apply_committed();
        }
    }

    fn on_ae_reply(self: &Arc<Self>, term: u64, ok: bool, idx: u64, by: u32) {
        let mut r = self.raft_lock();
        if term > r.term {
            r.become_follower(term);
            return;
        }
        if term != r.term || r.role != Role::Leader {
            return;
        }
        if ok {
            let prev = r.matched.get(&by).copied().unwrap_or(0);
            r.matched.insert(by, idx.max(prev));
            r.next.insert(by, idx + 1);
            // Advance the commit: the highest N with a majority at/above it, only for our term.
            let mut ms: Vec<u64> = r.matched.values().copied().collect();
            ms.push(r.last_index());
            ms.sort_unstable();
            let n = ms[ms.len() - Raft::quorum(self.n)];
            if n > r.commit && r.term_at(n) == r.term {
                r.commit = n;
            }
        } else {
            let next = r.next.get(&by).copied().unwrap_or(1).saturating_sub(1).max(1);
            // Too far behind for the log we keep: send the snapshot instead.
            if next <= r.base_index {
                let snap = r.snap.clone();
                let t = r.term;
                drop(r);
                if let Some(s) = snap {
                    self.send_to(by, &Msg::InstallSnapshot { term: t, leader: self.me, snap: s });
                }
                return;
            }
            r.next.insert(by, next);
        }
        let need_apply = r.commit > r.applied;
        let need_rep = r.role == Role::Leader;
        drop(r);
        if need_rep {
            self.replicate_to(by);
        }
        if need_apply {
            self.apply_committed();
        }
    }

    fn on_snapshot(self: &Arc<Self>, term: u64, leader: u32, snap: Snapshot) {
        let mut r = self.raft_lock();
        if term > r.term {
            r.become_follower(term);
        }
        if term != r.term {
            return;
        }
        r.leader = Some(leader);
        r.election_at = Instant::now() + Raft::election_timeout();
        if snap.last_index <= r.commit {
            return; // older than what we have
        }
        r.log.clear();
        r.base_index = snap.last_index;
        r.base_term = snap.last_term;
        r.commit = snap.last_index;
        r.applied = snap.last_index;
        r.snap = Some(snap.clone());
        let t = r.term;
        drop(r);
        println!("raft: installed snapshot at index {}", snap.last_index);
        self.sm.lock().unwrap().install(&snap);
        // A snapshot can carry a scan we didn't know: join or leave the work.
        self.after_snapshot();
        self.send_to(leader, &Msg::InstallSnapshotReply { term: t, by: self.me });
    }

    fn on_forward(self: &Arc<Self>, req: u64, from: u32, cmd: ForwardCmd) {
        let is_leader = self.raft_lock().role == Role::Leader;
        let node = self.clone();
        tokio::spawn(async move {
            let (ok, msg) = if is_leader {
                match cmd {
                    ForwardCmd::Start { from, to } => node.client_start_local(from, to).await,
                    ForwardCmd::Stop => node.client_stop_local().await,
                    ForwardCmd::Found { id, code } => {
                        node.append_found(&id, &code, from).await;
                        (true, "logged".to_string())
                    }
                }
            } else {
                (false, "not the leader".to_string())
            };
            node.send_to(from, &Msg::ForwardReply { req, ok, msg });
        });
    }

    /// A worker says this code is valid: the leader logs it (fire-and-forget; the finder retries).
    fn on_found_code(self: &Arc<Self>, id: &str, code: &str, by: u32) {
        if !self.is_leader() {
            return;
        }
        let running = {
            let sm = self.sm.lock().unwrap();
            matches!(&sm.scan, Some(s) if s.id == id && s.state == "running")
        };
        if !running {
            return;
        }
        let node = self.clone();
        let (id, code) = (id.to_string(), code.to_string());
        tokio::spawn(async move {
            node.append_found(&id, &code, by).await;
        });
    }

    /// Append a Found entry without waiting (the finder retries until the scan ends).
    async fn append_found(&self, id: &str, code: &str, by: u32) {
        // Don't log it twice.
        {
            let sm = self.sm.lock().unwrap();
            let dup = match &sm.scan {
                Some(s) => s.id != id || s.state != "running" || s.code == code,
                None => true,
            };
            if dup {
                return;
            }
        }
        let at = crate::coord::now_unix();
        let index = {
            let mut r = self.raft_lock();
            if r.role != Role::Leader {
                return;
            }
            let index = r.last_index() + 1;
            let term = r.term;
            r.log.push(Entry { term, index, cmd: Command::Found { id: id.to_string(), code: code.to_string(), by, at } });
            index
        };
        self.replicate_all();
        let _ = index;
    }

    // ---- replication ----

    /// Send AppendEntries to one peer (heartbeat if nothing new).
    pub fn replicate_to(&self, peer: u32) {
        let (term, pli, plt, entries, lc, leader) = {
            let r = self.raft_lock();
            if r.role != Role::Leader {
                return;
            }
            let next = r.next.get(&peer).copied().unwrap_or(1);
            if next <= r.base_index {
                // Needs the snapshot, not the log.
                let snap = r.snap.clone();
                let t = r.term;
                drop(r);
                if let Some(s) = snap {
                    self.send_to(peer, &Msg::InstallSnapshot { term: t, leader: self.me, snap: s });
                }
                return;
            }
            let pli = next - 1;
            let plt = r.term_at(pli);
            let entries: Vec<Entry> = r.log.iter().filter(|e| e.index >= next).cloned().collect();
            (r.term, pli, plt, entries, r.commit, self.me)
        };
        self.send_to(peer, &Msg::AppendEntries { term, leader, pli, plt, entries, lc });
    }

    pub fn replicate_all(&self) {
        let peers: Vec<u32> = self.peers.lock().unwrap().keys().copied().collect();
        for p in peers {
            self.replicate_to(p);
        }
    }

    /// Apply committed entries in order, running worker side-effects.
    pub fn apply_committed(&self) {
        loop {
            let entry = {
                let r = self.raft_lock();
                if r.applied >= r.commit {
                    break;
                }
                let idx = r.applied + 1;
                match r.entry(idx) {
                    Some(e) => e.clone(),
                    None => break, // covered by a snapshot
                }
            };
            let applied = self.sm.lock().unwrap().apply(&entry.cmd);
            {
                let mut r = self.raft_lock();
                r.applied = r.applied.max(entry.index);
                // A terminal entry clears any pending stop for its scan.
                if matches!(entry.cmd, Command::Stop { .. } | Command::Found { .. } | Command::End { .. }) {
                    r.stop_pending = None;
                }
            }
            match applied {
                Applied::ScanStarted(id) => self.join_work(&id),
                Applied::ScanEnded(_) => self.stop_work(),
                Applied::None => {}
            }
            // A finished scan: snapshot it and forget the log (it stays tiny).
            let finished = matches!(entry.cmd, Command::Found { .. } | Command::Stop { .. } | Command::End { .. });
            if finished {
                self.maybe_snapshot();
            }
        }
    }

    /// Once the scan is over and committed everywhere we need, snapshot the state and drop the log.
    fn maybe_snapshot(&self) {
        let mut r = self.raft_lock();
        if r.applied < r.commit {
            return;
        }
        let snap = self.sm.lock().unwrap().snapshot(r.commit, r.term_at(r.commit));
        let idx = snap.last_index;
        r.snap = Some(snap);
        r.log.clear();
        r.base_index = idx;
        r.base_term = r.term_at(idx);
        // Followers that missed it get the snapshot on next contact.
        for v in r.next.values_mut() {
            *v = (*v).max(idx + 1);
        }
        println!("raft: snapshotted at index {idx}, log cleared");
    }

    // ---- elections & ticks ----

    /// One tick: start an election when due, heartbeat when leader, retry replication.
    pub async fn raft_tick(self: Arc<Self>) {
        let mut tick = tokio::time::interval(TICK);
        loop {
            tick.tick().await;
            let now = Instant::now();
            enum Act {
                None,
                Elect,
                Beat,
            }
            let act = {
                let mut r = self.raft_lock();
                match r.role {
                    Role::Leader => {
                        if now.duration_since(r.hb_at) >= HEARTBEAT {
                            r.hb_at = now;
                            Act::Beat
                        } else {
                            Act::None
                        }
                    }
                    _ => {
                        if now >= r.election_at {
                            let me = self.me;
                            r.become_candidate(me);
                            Act::Elect
                        } else {
                            Act::None
                        }
                    }
                }
            };
            match act {
                Act::None => {}
                Act::Elect => {
                    let (term, lli, llt) = {
                        let r = self.raft_lock();
                        (r.term, r.last_index(), r.last_term())
                    };
                    println!("raft: replica {} starting election for term {term}", self.me);
                    self.broadcast(&Msg::RequestVote { term, cand: self.me, lli, llt });
                }
                Act::Beat => self.replicate_all(),
            }
            // If we just became leader with a single vote (one node), commit a no-op heartbeat entry
            // so the commit index can advance; harmless otherwise.
            self.apply_committed();
        }
    }

    // ---- client requests: start/stop from ANY replica ----

    /// Append a command as leader and wait for it to commit.
    async fn append_and_wait(&self, cmd: Command) -> (bool, String) {
        let index = {
            let mut r = self.raft_lock();
            if r.role != Role::Leader {
                return (false, "not the leader".to_string());
            }
            let index = r.last_index() + 1;
            let term = r.term;
            r.log.push(Entry { term, index, cmd });
            index
        };
        self.replicate_all();
        let ok = tokio::time::timeout(CLIENT_WAIT, async {
            loop {
                {
                    let r = self.raft_lock();
                    if r.commit >= index {
                        break;
                    }
                    if r.role != Role::Leader {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok();
        self.apply_committed();
        let still_leader = self.raft_lock().role == Role::Leader;
        let committed = self.raft_lock().commit >= index;
        (ok && still_leader && committed, if committed { String::new() } else { "not committed (lost leadership?)".to_string() })
    }

    fn validate_start(&self, from: u32, to: u32) -> Result<(), String> {
        if from >= to || to > 100_000 {
            return Err("need 0 <= from < to <= 100000".to_string());
        }
        let sm = self.sm.lock().unwrap();
        if let Some(s) = &sm.scan {
            if s.state == "running" {
                return Err(format!("scan {} already running ({}); only one scan at a time", s.id, s.range()));
            }
        }
        Ok(())
    }

    async fn client_start_local(&self, from: u32, to: u32) -> (bool, String) {
        if let Err(e) = self.validate_start(from, to) {
            return (false, e);
        }
        let id = format!("{:08x}", crate::coord::random_u64() as u32);
        let seed = crate::coord::random_u64();
        let at = crate::coord::now_unix();
        let range = format!("{from:05}..{to:05}");
        let (ok, e) = self.append_and_wait(Command::Start { id: id.clone(), from, to, seed, at }).await;
        if ok {
            // Another start may have won the race; only claim ours.
            let mine = self.sm.lock().unwrap().scan.as_ref().map(|s| s.id == id).unwrap_or(false);
            if mine {
                println!("scan {id}: started {range}");
                (true, format!("started scan {range}"))
            } else {
                (false, "another scan started instead".to_string())
            }
        } else {
            (false, e)
        }
    }

    async fn client_stop_local(&self) -> (bool, String) {
        let id = {
            let sm = self.sm.lock().unwrap();
            match &sm.scan {
                Some(s) if s.state == "running" => s.id.clone(),
                _ => return (true, "nothing running".to_string()),
            }
        };
        // Dedupe: the HTTP fan-out forwards one stop per replica; log it once.
        {
            let mut r = self.raft_lock();
            if r.stop_pending.as_deref() == Some(&id) {
                return (true, "stop already going through".to_string());
            }
            r.stop_pending = Some(id.clone());
        }
        let by = self.me;
        let at = crate::coord::now_unix();
        let (ok, e) = self.append_and_wait(Command::Stop { id, by, at }).await;
        (ok, if ok { "stop committed; every replica stops".to_string() } else { e })
    }

    /// GET /start, /orchestrate/start — from any replica: to the leader, or 503 while there is none.
    pub async fn client_start(self: &Arc<Self>, from: u32, to: u32) -> (bool, String) {
        self.client_request(ForwardCmd::Start { from, to }).await
    }

    /// GET /stop, /orchestrate/stop — from any replica. `fanout`: also fan out over plain HTTP, so a
    /// replica whose mesh is down still hears it (?local=1 skips the fan-out to avoid a storm).
    pub async fn client_stop(self: &Arc<Self>, fanout: bool) -> (bool, String) {
        if fanout {
            let node = self.clone();
            tokio::spawn(async move {
                let client = node.client.clone();
                for k in 1..=node.n {
                    if k == node.me {
                        continue;
                    }
                    let c = client.clone();
                    let url = format!("{}/stop?local=1", crate::replica_url(k));
                    tokio::spawn(async move {
                        let _ = c.get(&url).timeout(Duration::from_secs(5)).send().await;
                    });
                }
            });
        }
        self.client_request(ForwardCmd::Stop).await
    }

    pub async fn client_request(self: &Arc<Self>, cmd: ForwardCmd) -> (bool, String) {
        // Fast path: we are the leader.
        if self.raft_lock().role == Role::Leader {
            return match cmd {
                ForwardCmd::Start { from, to } => self.client_start_local(from, to).await,
                ForwardCmd::Stop => self.client_stop_local().await,
                ForwardCmd::Found { id, code } => {
                    let by = self.me;
                    self.append_found(&id, &code, by).await;
                    (true, "logged".to_string())
                }
            };
        }
        // Forward to whoever leads, waiting a little for an election to settle.
        let req = self.req_id.fetch_add(1, Ordering::SeqCst) + 1;
        let deadline = Instant::now() + Duration::from_secs(6);
        loop {
            let leader = self.raft_lock().leader;
            if let Some(l) = leader {
                let (tx, rx) = oneshot::channel();
                self.fwd.lock().unwrap().insert(req, tx);
                self.send_to(l, &Msg::Forward { req, from: self.me, cmd: cmd.clone() });
                match tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), rx).await {
                    Ok(Ok((false, msg))) if msg == "not the leader" => {
                        // It stepped down mid-flight; find the new leader and retry.
                        self.fwd.lock().unwrap().remove(&req);
                    }
                    Ok(Ok(r)) => return r,
                    _ => {
                        self.fwd.lock().unwrap().remove(&req);
                    }
                }
            }
            if Instant::now() >= deadline {
                return (false, "no leader right now; try again in a moment".to_string());
            }
            // Nudge an election if nobody leads.
            if self.raft_lock().leader.is_none() {
                self.raft_lock().election_at = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// A snapshot arrived carrying a scan we didn't know: join or leave the work accordingly.
    fn after_snapshot(&self) {
        let (id, running) = {
            let sm = self.sm.lock().unwrap();
            match &sm.scan {
                Some(s) => (s.id.clone(), s.state == "running"),
                None => (String::new(), false),
            }
        };
        if running {
            self.join_work(&id);
        } else {
            self.stop_work();
        }
    }
}
