import time
import json
import threading
import google.generativeai as genai
import socket

def network_server_request(cmd):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    try:
        s.connect(('localhost', 6000))
        s.sendall(cmd.encode('utf-8'))
        data = s.recv(4096)
        s.close()
        if not data:
            return {"error": "No response from network server"}
        return json.loads(data.decode('utf-8'))
    except Exception as e:
        return {"error": str(e)}

class PaxosNode:
    def __init__(self, server_id, all_servers, kv_store, send_func):
        self.server_id = server_id
        self.all_servers = all_servers
        self.kv_store = kv_store
        self.send_func = send_func

        self.op_num = 0
        self.is_leader = False
        self.known_leader = None
        self.current_ballot = None
        self.promised_ballot = None
        self.accepted_ballot = None
        self.accepted_value = None
        self.applied_operations_count = 0
        self.highest_seen_ballot = (0, 0, 0)
        self.did_leader_discovery = False
        self.last_printed_state = {}

        # For tracking proposals
        self.operation_queue = []

        # Maps from ballot to collected promises/accepteds
        self.promise_responses = {}
        self.accepted_responses = {}

        # Requests forwarded to leader and waiting for ACK
        self.pending_forwards = {}

        # candidate_answers[context_id][server_id] = answer
        self.candidate_answers = {}

        # Timeouts thread
        self.running = True
        self.timeout_thread = threading.Thread(target=self.check_timeouts_loop, daemon=True)
        self.timeout_thread.start()

    def check_timeouts_loop(self):
        while self.running:
            self.check_timeouts()
            time.sleep(1)

    def stop(self):
        self.running = False

    def handle_user_command(self, cmd_str):
        parts = cmd_str.strip().split()
        if len(parts) == 0:
            return
        cmd = parts[0]
        if cmd == "create":
            if len(parts) < 2:
                print("Usage: create <context_id>")
                return
            context_id = parts[1]
            self.submit_operation(f"createContext {context_id}")
        elif cmd == "query":
            if len(parts) < 3:
                print("Usage: query <context_id> <query_string>")
                return
            cid = parts[1]
            query_str = " ".join(parts[2:])
            self.submit_operation(f"query {cid} {query_str} {self.server_id}")
        elif cmd == "choose":
            if len(parts) < 3:
                print("Usage: choose <context_id> <candidate_server_id>")
                return
            cid = parts[1]
            candidate_server_id = parts[2]
            if cid in self.candidate_answers and candidate_server_id.isdigit() and int(candidate_server_id) in self.candidate_answers[cid]:
                chosen_answer = self.candidate_answers[cid][int(candidate_server_id)]
                self.submit_operation(f"answer {cid} {chosen_answer}")
                self.candidate_answers[cid].clear()
                if cid in self.last_printed_state:
                    del self.last_printed_state[cid]
            else:
                print("Candidate answer not found for that context and server.")
        elif cmd == "view":
            if len(parts) < 2:
                print("Usage: view <context_id>")
                return
            cid = parts[1]
            self.kv_store.view_context(cid)
        elif cmd == "viewall":
            self.kv_store.view_all()
        elif cmd == "exit":
            print("Exiting...")
            return "exit"
        else:
            print("Unknown command")

    def submit_operation(self, operation):
        if not self.did_leader_discovery:
            print(f"[Server {self.server_id}] Discovering current leader...")
            discover_msg = {
                "type": "LEADER_DISCOVER",
                "from": self.server_id,
                "to": "ALL"
            }
            self.broadcast_message(discover_msg)
            # Wait a bit for responses
            time.sleep(4)
            
            self.did_leader_discovery = True

        if not self.is_leader and self.known_leader is not None:
            forward_msg = {
                "type": "FORWARD",
                "from": self.server_id,
                "to": self.known_leader,
                "operation": operation
            }
            self.send_message(forward_msg, self.known_leader)
            # start timer
            self.pending_forwards[operation] = time.time()
        else:
            if not self.is_leader and self.known_leader is None:
                print(f"[Server {self.server_id}] No known leader. Becoming proposer.")
                self.start_leader_election()
                self.operation_queue.append(operation)
            else:
                # We are leader
                self.propose_operation(operation)

    def propose_operation(self, operation):
        if self.current_ballot is None:
            self.current_ballot = (1, self.server_id, self.op_num)

        self.op_num = self.current_ballot[2]
        ballot = self.current_ballot

        if ballot not in self.accepted_responses:
            self.accepted_responses[ballot] = []

        self_accepted = {
            "type": "ACCEPTED",
            "from": self.server_id,
            "to": "ALL",
            "ballot": ballot,
            "operation": operation
        }

        self.accepted_responses[ballot].append(self_accepted)

        print(f"[Server {self.server_id}] Sending ACCEPT {ballot} {operation} to ALL")
        accept_msg = {
            "type": "ACCEPT",
            "from": self.server_id,
            "to": "ALL",
            "ballot": ballot,
            "operation": operation
        }
        self.broadcast_message(accept_msg)

        self.current_ballot = (ballot[0], ballot[1], ballot[2] + 1)
        self.op_num = self.current_ballot[2]

    def start_leader_election(self):
        next_seq = self.highest_seen_ballot[0] + 1
        self.current_ballot = (next_seq, self.server_id, self.applied_operations_count)

        # Clear any old promise responses for this ballot
        bkey = self.current_ballot
        self.promise_responses[bkey] = []
        
        # Add our own promise to our response set
        self_promise = {
            "type": "PROMISE",
            "from": self.server_id,
            "ballot": self.current_ballot,
            "accepted_ballot": self.accepted_ballot,
            "accepted_value": self.accepted_value
        }
        self.promise_responses[bkey].append(self_promise)

        print(f"[Server {self.server_id}] Sending PREPARE {self.current_ballot} to ALL")
        prepare_msg = {
            "type": "PREPARE",
            "from": self.server_id,
            "to": "ALL",
            "ballot": self.current_ballot
        }
        self.broadcast_message(prepare_msg)

    def handle_message(self, msg):
        mtype = msg.get("type")
        if mtype == "FORWARD":
            op = msg["operation"]
            ack_msg = {
                "type": "ACK",
                "from": self.server_id,
                "to": msg["from"],
                "operation": op
            }
            print(f"[Server {self.server_id}] Received FORWARD {op} from Server {msg['from']}, sending ACK")
            self.send_message(ack_msg, msg["from"])
            if self.is_leader:
                self.propose_operation(op)
        elif mtype == "ACK":
            print(f"[Server {self.server_id}] Received ACK for operation {msg['operation']} from Leader")
            if msg["operation"] in self.pending_forwards:
                del self.pending_forwards[msg["operation"]]
        elif mtype == "PREPARE":
            self.on_prepare(msg)
        elif mtype == "PROMISE":
            self.on_promise(msg)
        elif mtype == "ACCEPT":
            self.on_accept(msg)
        elif mtype == "ACCEPTED":
            self.on_accepted(msg)
        elif mtype == "DECIDE":
            self.on_decide(msg)
        elif mtype == "CANDIDATE_ANSWER":
            cid = msg["cid"]
            candidate_answer = msg["candidate_answer"]
            from_sid = msg["from"]

            if cid not in self.candidate_answers:
                self.candidate_answers[cid] = {}

            self.candidate_answers[cid][from_sid] = candidate_answer
            self.check_and_print_all_candidates(cid)
        elif mtype == "LEADER_ANNOUNCE":
            leader_id = msg["leader_id"]
            self.known_leader = leader_id
            print(f"[Server {self.server_id}] Updated known leader to Server {leader_id}")
        elif mtype == "LEADER_DISCOVER":
            if self.known_leader is not None:
                response = {
                    "type": "LEADER_ANNOUNCE",
                    "from": self.server_id,
                    "to": msg["from"],
                    "leader_id": self.known_leader
                }
                self.send_message(response, msg["from"])

    def on_prepare(self, msg):
        incoming_ballot = tuple(msg["ballot"])
        print(f"[Server {self.server_id}] Received PREPARE {incoming_ballot} from Server {msg['from']}")
        
        # Update highest seen ballot
        if incoming_ballot > self.highest_seen_ballot:
            self.highest_seen_ballot = incoming_ballot

        # We only check if the incoming ballot is higher than promised_ballot
        if self.promised_ballot is None or incoming_ballot > self.promised_ballot:
            self.promised_ballot = incoming_ballot
            response = {
                "type": "PROMISE",
                "from": self.server_id,
                "to": msg["from"],
                "ballot": incoming_ballot,
                "accepted_ballot": self.accepted_ballot,
                "accepted_value": self.accepted_value
            }
            self.send_message(response, msg["from"])
            print(f"[Server {self.server_id}] Sending PROMISE {incoming_ballot} to Server {msg['from']}")

    def on_promise(self, msg):
        print(f"[Server {self.server_id}] Received PROMISE {msg['ballot']} from Server {msg['from']}")
        bkey = tuple(msg["ballot"])
        if bkey not in self.promise_responses:
            self.promise_responses[bkey] = []
        self.promise_responses[bkey].append(msg)

        # Only become leader once
        if not self.is_leader and len(self.promise_responses[bkey]) >= 2:  # Add check for not already leader
            self.is_leader = True
            self.known_leader = self.server_id
            print(f"[Server {self.server_id}] Became leader with ballot {bkey}")
            leader_announce_msg = {
                "type": "LEADER_ANNOUNCE",
                "from": self.server_id,
                "to": "ALL",
                "leader_id": self.server_id
            }
            self.broadcast_message(leader_announce_msg)

            # Process queued operations
            for op in self.operation_queue:
                self.propose_operation(op)
            self.operation_queue.clear()

    def on_accept(self, msg):
        print(f"[Server {self.server_id}] Received ACCEPT {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        incoming_ballot = tuple(msg["ballot"])

        if incoming_ballot > self.highest_seen_ballot:
            self.highest_seen_ballot = incoming_ballot

        if self.promised_ballot is None or incoming_ballot >= self.promised_ballot:
            self.accepted_ballot = incoming_ballot
            self.accepted_value = msg["operation"]
            accepted_msg = {
                "type": "ACCEPTED",
                "from": self.server_id,
                "to": msg["from"],
                "ballot": incoming_ballot,
                "operation": msg["operation"]
            }
            print(f"[Server {self.server_id}] Sending ACCEPTED {incoming_ballot} {msg['operation']} to Server {msg['from']}")
            self.send_message(accepted_msg, msg["from"])

    def on_accepted(self, msg):
        print(f"[Server {self.server_id}] Received ACCEPTED {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        bkey = tuple(msg["ballot"])
        if bkey not in self.accepted_responses:
            self.accepted_responses[bkey] = []
        
        # Check if we already have this response
        for resp in self.accepted_responses[bkey]:
            if resp["from"] == msg["from"]:
                return  # Don't process duplicate ACCEPTED messages
        
        self.accepted_responses[bkey].append(msg)

        if not hasattr(self, 'decided_ballots'):
            self.decided_ballots = set()

        # Only proceed if we haven't already decided this ballot and we have majority
        if bkey not in self.decided_ballots and len(self.accepted_responses[bkey]) >= 2:
            self.decided_ballots.add(bkey)
            decide_msg = {
                "type": "DECIDE",
                "from": self.server_id,
                "to": "ALL",
                "ballot": bkey,
                "operation": msg["operation"]
            }
            print(f"[Server {self.server_id}] Sending DECIDE {bkey} {msg['operation']} to ALL")
            self.broadcast_message(decide_msg)
            self.apply_operation(msg["operation"])

    def on_decide(self, msg):
        print(f"[Server {self.server_id}] Received DECIDE {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        op = msg["operation"]
        self.apply_operation(op)

    def apply_operation(self, op):
        if not hasattr(self, 'applied_ops'):
            self.applied_ops = set()
        
        # Create a unique key for this operation
        op_key = f"{op}_{self.op_num}"
        if op_key in self.applied_ops:
            return  # Skip if we've already applied this operation
            
        self.applied_ops.add(op_key)
        parts = op.split()
        if parts[0] == "createContext":
            cid = parts[1]
            self.kv_store.create_context(cid)
        elif parts[0] == "query":
            cid = parts[1]
            query_str = " ".join(parts[2:-1])
            originating_server = parts[-1]
            origin_id = int(originating_server)

            if cid not in self.candidate_answers:
                self.candidate_answers[cid] = {}
            self.kv_store.add_query(cid, query_str)
            full_context = self.kv_store.get_full_context(cid)
            prompt = full_context + "Answer: "
            try:
                response = self.llm_generate(prompt)
                candidate_answer = response.strip()
                self.candidate_answers[cid][self.server_id] = candidate_answer

                # Only check/print if we're the originating server
                if self.server_id == origin_id:
                    # Store our own answer but don't print yet
                    pass
                else:
                    # Send to originating server
                    msg = {
                        "type": "CANDIDATE_ANSWER",
                        "from": self.server_id,
                        "to": origin_id,
                        "cid": cid,
                        "candidate_answer": candidate_answer
                    }
                    self.send_message(msg, origin_id)

            except Exception as e:
                print(f"[Server {self.server_id}] Error querying LLM: {e}")

        elif parts[0] == "answer":
            cid = parts[1]
            answer_str = " ".join(parts[2:])
            self.kv_store.add_answer(cid, answer_str)

        self.applied_operations_count += 1

    def llm_generate(self, prompt):
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        return response.text

    def broadcast_message(self, msg):
        for sid, addr in self.all_servers.items():
            if sid != self.server_id:
                self.send_message(msg, sid)

    def send_message(self, msg, sid=None):
        # Before actually sending, check link and node states via network_server_request in send_func
        if sid is None:
            dest = msg.get("to")
            if dest == "ALL":
                for sid2, addr2 in self.all_servers.items():
                    if sid2 != self.server_id:
                        self.send_message(msg, sid2)
                return
            else:
                sid = dest

        if sid not in self.all_servers:
            return

        data = json.dumps(msg).encode('utf-8')
        # send_func checks node and link states automatically
        self.send_func(sid, data)

    def check_timeouts(self):
        now = time.time()
        timeout_seconds = 10
        for op, start_time in list(self.pending_forwards.items()):
            if now - start_time > timeout_seconds:
                print(f"[Server {self.server_id}] TIMEOUT waiting for ACK on operation {op}")
                del self.pending_forwards[op]
                self.start_leader_election()
                self.operation_queue.append(op)

    def check_and_print_all_candidates(self, cid):
        if cid in self.candidate_answers and len(self.candidate_answers[cid]) > 0:
            # Create a snapshot of current answers
            current_state = frozenset(
                (sid, ans) 
                for sid, ans in sorted(self.candidate_answers[cid].items())
            )
            
            # Only print if this state hasn't been printed before
            if cid not in self.last_printed_state or current_state != self.last_printed_state[cid]:
                print(f"\nCandidate answers for context {cid}:")
                for sid in sorted(self.candidate_answers[cid].keys()):
                    ans = self.candidate_answers[cid][sid]
                    print(f"Context {cid} - Candidate {sid}: {ans}")
                print()
                
                # Update the last printed state
                self.last_printed_state[cid] = current_state