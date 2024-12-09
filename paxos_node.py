import time
import json
import threading
import google.generativeai as genai

class PaxosNode:
    def __init__(self, server_id, all_servers, kv_store, network_server, send_func):
        self.server_id = server_id
        self.all_servers = all_servers
        self.kv_store = kv_store
        self.network_server = network_server
        self.send_func = send_func

        self.op_num = 0
        self.is_leader = False
        self.known_leader = None
        self.current_ballot = None
        self.promised_ballot = None
        self.accepted_ballot = None
        self.accepted_value = None
        self.applied_operations_count = 0

        # For tracking proposals
        self.operation_queue = []

        # Map from ballot to collected promises/accepteds
        self.promise_responses = {}
        self.accepted_responses = {}

        # Requests forwarded to leader and waiting for ACK
        self.pending_forwards = {}

        # For storing candidate answers: candidate_answers[context_id][server_id] = answer
        self.candidate_answers = {}

        # Thread for periodically checking timeouts
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
        if not self.is_leader and self.known_leader is not None:
            forward_msg = {
                "type": "FORWARD",
                "from": self.server_id,
                "to": self.known_leader,
                "operation": operation
            }
            self.send_message(forward_msg, self.known_leader)
            # start timer
            import time
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

        ballot = (self.current_ballot[0], self.current_ballot[1], self.op_num)
        accept_msg = {
            "type": "ACCEPT",
            "from": self.server_id,
            "to": "ALL",
            "ballot": ballot,
            "operation": operation
        }
        print(f"[Server {self.server_id}] Sending ACCEPT {ballot} {operation} to ALL")
        self.broadcast_message(accept_msg)
        self.op_num += 1

    def start_leader_election(self):
        if self.current_ballot is None:
            self.current_ballot = (1, self.server_id, 0)
        else:
            self.current_ballot = (self.current_ballot[0] + 1, self.server_id, 0)

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

    def on_prepare(self, msg):
        incoming_ballot = tuple(msg["ballot"])
        print(f"[Server {self.server_id}] Received PREPARE {incoming_ballot} from Server {msg['from']}")
        incoming_op_num = incoming_ballot[2]

        if self.applied_operations_count > incoming_op_num:
            return

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

        if len(self.promise_responses[bkey]) >= 2:
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
            for op in self.operation_queue:
                self.propose_operation(op)
            self.operation_queue.clear()

    def on_accept(self, msg):
        print(f"[Server {self.server_id}] Received ACCEPT {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        incoming_ballot = tuple(msg["ballot"])
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
        self.accepted_responses[bkey].append(msg)

        if len(self.accepted_responses[bkey]) >= 2:
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
        parts = op.split()
        if parts[0] == "createContext":
            cid = parts[1]
            self.kv_store.create_context(cid)
        elif parts[0] == "query":
            cid = parts[1]
            query_str = " ".join(parts[2:-1])
            originating_server = parts[-1]
            origin_id = int(originating_server)
            self.kv_store.add_query(cid, query_str)

            full_context = self.kv_store.get_full_context(cid)
            prompt = full_context + "Answer: "
            try:
                response = self.llm_generate(prompt)
                candidate_answer = response.strip()
                if cid not in self.candidate_answers:
                    self.candidate_answers[cid] = {}
                self.candidate_answers[cid][self.server_id] = candidate_answer

                if self.server_id != origin_id:
                    msg = {
                        "type": "CANDIDATE_ANSWER",
                        "from": self.server_id,
                        "to": origin_id,
                        "cid": cid,
                        "candidate_answer": candidate_answer
                    }
                    self.send_message(msg, origin_id)
                else:
                    self.check_and_print_all_candidates(cid)

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
                self.network_server.send_message(self.server_id, sid, json.dumps(msg).encode('utf-8'), self.send_func)

    def send_message(self, msg, sid=None):
        if sid is None:
            dest = msg.get("to")
            if dest == "ALL":
                self.broadcast_message(msg)
                return
            else:
                sid = dest
        if sid not in self.all_servers:
            return
        data = json.dumps(msg).encode('utf-8')
        # Use network_server to send message with delay and link checks
        self.network_server.send_message(self.server_id, sid, data, self.send_func)

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
        if cid in self.candidate_answers and len(self.candidate_answers[cid]) == 3:
            for sid in sorted(self.candidate_answers[cid].keys()):
                ans = self.candidate_answers[cid][sid]
                print(f"\nContext {cid} - Candidate {sid}: {ans}")
            print()
