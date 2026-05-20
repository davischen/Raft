from concurrent import futures
import configparser
import logging
import os
import random
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

import grpc
import raft_pb2
import raft_pb2_grpc
import json


CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")

RPC_DEADLINE_SEC = 3.0


def read_cfg():
    cfg = configparser.ConfigParser()
    if not os.path.exists(CONFIG_PATH):
        raise RuntimeError("config.ini not found")
    cfg.read(CONFIG_PATH)
    return cfg

class Server(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id: int):
        self.server_id = int(server_id)
        self.store = {}
        self.state_lock = threading.RLock()
        cfg = read_cfg()

        # ==== Raft status ====
        self.currentTerm: int = 0
        self.votedFor: Optional[int] = None
        self.currentRole: str = "follower"         
        self.currentLeader: Optional[int] = None
        self.commitIndex: int = 0
        self.lastApplied: int = 0

        self.persistence_state_path = cfg.get("Servers", "persistent_state_path", fallback="memory").strip()

        # Build peer map from config
        self.peer_map: Dict[int, Tuple[str, int]] = {}
        base_addr = cfg.get("Global", "base_address", fallback="127.0.0.1")
        base_port = cfg.getint("Servers", "base_port", fallback=9001)
        active_ids = [int(s.strip()) for s in cfg.get("Servers", "active", fallback="").split(",") if s.strip()]
        for sid in active_ids:
            self.peer_map[sid] = (base_addr, base_port + sid)

        self.server_port = base_port + self.server_id
        self.peer_ids = [sid for sid in self.peer_map if sid != self.server_id]
        logging.info(f"S{self.server_id}@{self.server_port} up as follower; peers={self.peer_ids}")


        self.log: List[raft_pb2.LogEntry] = [None]
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        self._load_state()

        self._channels: Dict[int, grpc.Channel] = {}
        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_timer: Optional[threading.Timer] = None
        self.reset_election_timer()


    # -------- timers --------
    def reset_election_timer(self):
        if self._election_timer:
            self._election_timer.cancel()
        t = random.uniform(150, 300) / 1000.0
        self._election_timer = threading.Timer(t, self._on_election_timeout)
        self._election_timer.start()
        logging.debug(f"S{self.server_id} reset election timer to {t*1000:.0f}ms")
    
    def _start_heartbeat_timer(self):
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
        self._heartbeat_timer = threading.Timer(75 / 1000.0, self._broadcast_append_entries)
        self._heartbeat_timer.start()

    def _cancel_heartbeat_timer(self):
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    # -------- role transitions --------
    def become_follower(self, new_term: Optional[int] = None, leader_Id: Optional[int] = None):
        with self.state_lock:
            if new_term is not None and new_term > self.currentTerm:
                self.currentTerm = new_term
                self.votedFor = None
                self._persist_state()
            if self.currentRole != "follower":
                logging.info(f"S{self.server_id} -> follower (term {self.currentTerm}), leader={leader_Id}")
            self.currentRole = "follower"
            self.currentLeader = leader_Id
            self._cancel_heartbeat_timer()
            self.reset_election_timer()

    def become_candidate(self):
        with self.state_lock:
            self.currentTerm += 1
            self.votedFor = self.server_id
            self.currentRole = "candidate"
            self._persist_state()
            term = self.currentTerm
            logging.info(f"S{self.server_id} -> candidate (term {term}), voted for self")
        self.reset_election_timer()
        self._request_votes(term)

    def become_leader(self):
        with self.state_lock:
            if self.currentRole != "candidate":
                return
            logging.info(f"S{self.server_id} -> LEADER (term {self.currentTerm})")
            self.currentRole = "leader"
            self.currentLeader = self.server_id

            noop = raft_pb2.LogEntry(term=self.currentTerm, key="", value="", clientId=0, requestId=0)
            self.log.append(noop)
            self._persist_state()

            last_log_idx = len(self.log) - 1
            for peer_id in self.peer_ids:
                self.next_index[peer_id] = last_log_idx + 1
                self.match_index[peer_id] = 0

        self._broadcast_append_entries()

        self._cancel_heartbeat_timer()
        self._start_heartbeat_timer()

    # -------- election & broadcast --------
    def _on_election_timeout(self):
        with self.state_lock:
            role = self.currentRole
        if role == "leader":
            self.reset_election_timer()
            return
        logging.debug(f"S{self.server_id} election timeout (role={role}) -> start election")
        self.become_candidate()

    def _broadcast_append_entries(self):
        with self.state_lock:
            if self.currentRole != "leader":
                return

        def receiving(peer_id: int):
            try:

                with self.state_lock:
                    current_term = self.currentTerm
                    prevLogIdx = self.next_index[peer_id] - 1
                    prevLogTerm = 0
                    if prevLogIdx > 0:
                        prevLogTerm = self.log[prevLogIdx].term
                    leaderCommit = self.commitIndex
                    log_entries_for_peer = self.log[self.next_index[peer_id]:]

                args = raft_pb2.AppendEntriesArgs(
                    term=current_term,
                    leaderId=self.server_id,
                    prevLogIndex=prevLogIdx,
                    prevLogTerm=prevLogTerm,
                    entries=log_entries_for_peer,
                    leaderCommit=leaderCommit
                )

                if peer_id not in self._channels:
                    self._channels[peer_id] = grpc.insecure_channel(f"{self.peer_map[peer_id][0]}:{self.peer_map[peer_id][1]}")
                reply = raft_pb2_grpc.KeyValueStoreStub(self._channels[peer_id]).AppendEntries(args, timeout=RPC_DEADLINE_SEC)

                with self.state_lock:
                    if reply.term > self.currentTerm:
                        logging.info(f"S{self.server_id} saw higher term {reply.term} from {peer_id}, step down")
                        self.become_follower(new_term=reply.term)
                        return
                    
                    if reply.success:
                        entries_sent = len(log_entries_for_peer)
                        self.match_index[peer_id] = prevLogIdx + entries_sent
                        self.next_index[peer_id] = prevLogIdx + entries_sent + 1
                        self.update_commit_idx()
                    else:
                        old = self.next_index[peer_id]
                        self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
                        logging.info(f"failed S{peer_id}: next_index {old} ->{self.next_index[peer_id]}")
                        

            except Exception as e:
                logging.debug(f"S{self.server_id} HB->{peer_id} failed: {e}")

        for pid in self.peer_ids:
            threading.Thread(target=receiving, args=(pid,), daemon=True).start()

        self._start_heartbeat_timer()

    def _request_votes(self, election_term: int):
        votes = {self.server_id: True}
        needed = (len(self.peer_map) // 2) + 1
        votes_lock = threading.Lock()
        def receiving(peer_id: int):
            try:
                if peer_id not in self._channels:
                    self._channels[peer_id] = grpc.insecure_channel(f"{self.peer_map[peer_id][0]}:{self.peer_map[peer_id][1]}")

                with self.state_lock:
                    last_idx = len(self.log) - 1
                    last_term = self.log[last_idx].term if last_idx > 0 else 0

                reply = raft_pb2_grpc.KeyValueStoreStub(self._channels[peer_id]).RequestVote(
                    raft_pb2.RequestVoteArgs(
                        term=election_term,
                        candidateId=self.server_id,
                        lastLogIndex=last_idx,
                        lastLogTerm=last_term,
                    ),
                    timeout=RPC_DEADLINE_SEC,
                )
                with votes_lock:
                        votes[peer_id] = bool(reply.voteGranted)
                        granted_now = sum(1 for x in votes.values() if x)

                with self.state_lock:
                    if self.currentRole == "candidate" and self.currentTerm == election_term:
                        votesRecived = sum(1 for x in votes.values() if x)
                        if votesRecived >= needed:
                            self.become_leader()
                    elif reply.term > self.currentTerm:
                        self.become_follower(new_term=reply.term)
                        logging.debug(
                        f"S{self.server_id} vote from {peer_id}: {reply.voteGranted} "
                        f"({granted_now}/{needed})"
                    )

            except Exception as e:
                logging.debug(f"S{self.server_id} vote RPC to {peer_id} failed: {e}")

        for pid in self.peer_ids:
            threading.Thread(target=receiving, args=(pid,), daemon=True).start()

    # -------- gRPC --------
    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        with self.state_lock:
            return raft_pb2.State(
                term=self.currentTerm,
                isLeader=(self.currentRole == "leader"),
                commitIndex=self.commitIndex,
                lastApplied=self.lastApplied,
            )
    
    def Get(self, request, context):
        key = request.arg
        with self.state_lock:
            value = self.store.get(key, "")
            return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        key = request.key
        value = request.value
        with self.state_lock:
            if self.currentRole != "leader":
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Not leader")
                return raft_pb2.GenericResponse(success=False, error="Not leader")

            entry = raft_pb2.LogEntry(
                    term=self.currentTerm,
                    key=request.key,
                    value=request.value,
                    clientId=request.clientId,
                    requestId=request.requestId
            )

            self.log.append(entry)
            self._persist_state()
            log_index = len(self.log) - 1

        self._broadcast_append_entries()

        deadline = time.time() + 5.0
        while time.time() < deadline:
            with self.state_lock:
                if self.currentRole != "leader":
                    return raft_pb2.GenericResponse(success=False, error="Not leader")

                if self.commitIndex >= log_index:
                    return raft_pb2.GenericResponse(success=True)

            time.sleep(0.01)
        return raft_pb2.GenericResponse(success=False, error="operation timeout")

    def RequestVote(self, request, context):
        with self.state_lock:
            if request.term > self.currentTerm:
                self.become_follower(new_term=request.term)

            #vote_granted = False
            #if request.term < self.currentTerm:
            #    vote_granted = False
            #else:
            #    if self.votedFor in (None, request.candidateId):
            #        vote_granted = True
            #        self.votedFor = request.candidateId
            #        self.reset_election_timer()

            vote_granted = False
            if request.term >= self.currentTerm and (self.votedFor is None or self.votedFor == request.candidateId):
                log_idx = len(self.log) - 1
                last_term = self.log[log_idx].term if log_idx > 0 else 0
                log_ok = False
                if request.lastLogTerm > last_term:
                    log_ok = True
                elif request.lastLogTerm == last_term and request.lastLogIndex >= log_idx:
                    log_ok = True

                if log_ok:
                    vote_granted = True
                    self.votedFor = request.candidateId
                    self._persist_state()
                    logging.info(f"S{self.server_id} voted for -> {self.votedFor}")
                    self.reset_election_timer()
                else:
                    logging.info(f"S{self.server_id} rejected vote for {request.candidateId}")

            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=vote_granted)

    def AppendEntries(self, request, context):
        with self.state_lock:
            if request.term < self.currentTerm:
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)

            if request.term > self.currentTerm:
                self.currentTerm = request.term
                self.votedFor = None

            self.currentRole = "follower"
            self.currentLeader = request.leaderId
            self.reset_election_timer()

            if request.prevLogIndex >= len(self.log):
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)

            if request.prevLogIndex > 0:
                if (request.prevLogIndex >= len(self.log) or
                    self.log[request.prevLogIndex].term != request.prevLogTerm):
                    return raft_pb2.AppendEntriesReply(
                            term=self.currentTerm,
                            success=False
                    )

            for i, entry in enumerate(request.entries):
                log_index = request.prevLogIndex + i + 1
                if log_index < len(self.log):
                    if self.log[log_index].term != entry.term:
                        self.log = self.log[:log_index]
                        self.log.append(entry)
                        self._persist_state()
                else:
                    self.log.append(entry)
                    self._persist_state()

            if request.leaderCommit > self.commitIndex:
                self.commitIndex = min(request.leaderCommit, len(self.log) - 1)
                self.apply_committed_entries()

            logging.info(
                f"[S{self.server_id}] AE reply: term={self.currentTerm}, "
                f"myLogLen={len(self.log)}"
            )
            return raft_pb2.AppendEntriesReply(
                term=self.currentTerm,
                success=True
            )

    def apply_committed_entries(self):
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            entry = self.log[self.lastApplied]
            self.store[entry.key] = entry.value

    def update_commit_idx(self):
        if self.currentRole != "leader":
            return
        
        for n in range(len(self.log)-1, self.commitIndex, -1):
            if self.log[n].term != self.currentTerm:
                continue
            
            count = 1  # Leader has it
            for peer_id in self.peer_ids:
                if self.match_index.get(peer_id, 0) >= n:
                    count += 1
            
            majority = (len(self.peer_map) // 2) + 1
            if count >= majority:
                self.commitIndex = n
        
        self.apply_committed_entries()

    # persistence
    def _get_state_file(self):
        if self.persistence_state_path == "memory":
            return None
        return os.path.join(self.persistence_state_path, f"server_{self.server_id}.json")

    def _persist_state(self):
        state_file = self._get_state_file()
        if state_file is None:
            return

        temp_file = f"{state_file}.tmp"
        with open(temp_file, "w") as f:
            # assume caller holds state lock
            state = {
                "currentTerm": self.currentTerm,
                "votedFor": self.votedFor,
                "log": []
            }

            for i in range(1, len(self.log)):
                entry = self.log[i]
                state["log"].append({
                    "term": entry.term,
                    "key": entry.key,
                    "value": entry.value,
                    "clientId": entry.clientId,
                    "requestId": entry.requestId
                })

            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())

        os.rename(temp_file, state_file)

    def _load_state(self):
        state_file = self._get_state_file()
        if state_file is None or not os.path.exists(state_file):
            return False

        with open(state_file, "r") as f:
            data = json.load(f)

            with self.state_lock:
                 logging.info("loading from state file")
                 self.currentTerm = data.get("currentTerm", 0)
                 self.votedFor = data.get("votedFor", None)


                 self.log = [None]
                 for entry_dict in data.get("log", []):
                     entry = raft_pb2.LogEntry(
                         term=entry_dict["term"],
                         key=entry_dict["key"],
                         value=entry_dict["value"],
                         clientId=entry_dict.get("clientId", 0),
                         requestId=entry_dict.get("requestId", 0)
                     )
                     self.log.append(entry)

                 logging.info(f"loaded: term={self.currentTerm}, self.votedFor={self.votedFor} log_entries={len(self.log)}")
        return True

if __name__ == "__main__":
    server_id = int(sys.argv[1])
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [S{server_id}] %(levelname)s: %(message)s")
    
    cfg = read_cfg()
    base_addr = cfg.get("Global", "base_address", fallback="127.0.0.1")
    base_port = cfg.getint("Servers", "base_port", fallback=9001)
    max_workers = cfg.getint("Servers", "max_workers", fallback=10)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(Server(server_id), server)

    port = base_port + int(server_id)
    server.add_insecure_port(f"{base_addr}:{port}")
    server.start()
    #server.wait_for_termination()

    logging.info(f"Server {server_id} listening on {base_addr}:{port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
