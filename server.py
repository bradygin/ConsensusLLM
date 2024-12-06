import socket
import threading
import sys
import json
import time

# Hard-coded server addresses for demonstration
SERVER_ADDRESSES = {
    1: ('localhost', 5001),
    2: ('localhost', 5002),
    3: ('localhost', 5003)
}

class KeyValueStore:
    def __init__(self):
        self.contexts = {}
    
    def create_context(self, cid):
        if cid not in self.contexts:
            self.contexts[cid] = ""
        print(f"NEW CONTEXT {cid}")
    
    def add_query(self, cid, query_str):
        if cid not in self.contexts:
            self.contexts[cid] = ""
        self.contexts[cid] += f"Query: {query_str}\n"
        print(f"NEW QUERY on {cid} with {self.contexts[cid]}")

    def add_answer(self, cid, answer_str):
        if cid not in self.contexts:
            self.contexts[cid] = ""
        self.contexts[cid] += f"Answer: {answer_str}\n"
        print(f"CHOSEN ANSWER on {cid} with {answer_str}")

    def view_context(self, cid):
        if cid in self.contexts:
            print(self.contexts[cid])
        else:
            print(f"Context {cid} not found.")

    def view_all(self):
        for cid, ctx in self.contexts.items():
            print(f"view {cid}")
            print(ctx)


class PaxosNode:
    def __init__(self, server_id, all_servers, kv_store):
        self.server_id = server_id
        self.all_servers = all_servers
        self.kv_store = kv_store

        self.op_num = 0
        self.is_leader = False
        self.known_leader = None
        self.current_ballot = None
        self.promised_ballot = None
        self.accepted_ballot = None
        self.accepted_value = None
        
        # For tracking proposals
        self.operation_queue = []
        
        # Map from ballot to collected promises/accepteds
        self.promise_responses = {}
        self.accepted_responses = {}

        # Requests forwarded to leader and waiting for ACK
        self.pending_forwards = {}

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
                print("Usage: choose <context_id> <answer_string>")
                return
            cid = parts[1]
            # Use all parts after cid as the chosen answer
            chosen_answer = " ".join(parts[2:])
            self.submit_operation(f"answer {cid} {chosen_answer}")
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
        # If not leader and we know the leader, forward the request
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
            # If we are leader or no known leader, handle accordingly
            if not self.is_leader and self.known_leader is None:
                # No known leader, try to become one
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
            # Leader responds with ACK
            ack_msg = {
                "type": "ACK",
                "from": self.server_id,
                "to": msg["from"],
                "operation": op
            }
            print(f"[Server {self.server_id}] Received FORWARD {op} from Server {msg['from']}, sending ACK")
            self.send_message(ack_msg, msg["from"])
            # Propose the forwarded operation
            if self.is_leader:
                self.propose_operation(op)
        elif mtype == "ACK":
            print(f"[Server {self.server_id}] Received ACK for operation {msg['operation']} from Leader")
            # Operation acknowledged, remove from pending_forwards
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

    def on_prepare(self, msg):
        incoming_ballot = tuple(msg["ballot"])
        print(f"[Server {self.server_id}] Received PREPARE {incoming_ballot} from Server {msg['from']}")
        
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
            # Become leader
            self.is_leader = True
            self.known_leader = self.server_id
            print(f"[Server {self.server_id}] Became leader with ballot {bkey}")
            # Propose any queued operations
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
            # Majority accepted
            decide_msg = {
                "type": "DECIDE",
                "from": self.server_id,
                "to": "ALL",
                "ballot": bkey,
                "operation": msg["operation"]
            }
            print(f"[Server {self.server_id}] Sending DECIDE {bkey} {msg['operation']} to ALL")
            self.broadcast_message(decide_msg)

            # Leader applies operation locally
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
            query_str = " ".join(parts[2:-1]) # last part is originating server
            self.kv_store.add_query(cid, query_str)
        elif parts[0] == "answer":
            cid = parts[1]
            answer_str = " ".join(parts[2:])
            self.kv_store.add_answer(cid, answer_str)

    def broadcast_message(self, msg):
        for sid, addr in self.all_servers.items():
            if sid != self.server_id:
                self.send_message(msg, sid)

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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect(self.all_servers[sid])
            sock.sendall(data)
        except Exception as e:
            print(f"[Server {self.server_id}] Error sending message to {sid}: {e}")
        finally:
            sock.close()

    def check_timeouts(self):
        now = time.time()
        timeout_seconds = 5
        # If no ACK from leader for forwarded operations, timeout
        for op, start_time in list(self.pending_forwards.items()):
            if now - start_time > timeout_seconds:
                print(f"[Server {self.server_id}] TIMEOUT waiting for ACK on operation {op}")
                del self.pending_forwards[op]
                self.start_leader_election()
                self.operation_queue.append(op)

class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.all_servers = SERVER_ADDRESSES
        self.kv_store = KeyValueStore()
        self.node = PaxosNode(server_id, self.all_servers, self.kv_store)
        self.running = True

    def start(self):
        t = threading.Thread(target=self.listen, daemon=True)
        t.start()

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(self.all_servers[self.server_id])
            s.listen()
            print(f"[Server {self.server_id}] Listening on {self.all_servers[self.server_id]} ...")
            while self.running:
                try:
                    conn, addr = s.accept()
                    threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
                except:
                    continue


    def handle_client(self, conn, addr):
        data = conn.recv(4096)
        if not data:
            conn.close()
            return
        msg = json.loads(data.decode('utf-8'))
        self.node.handle_message(msg)
        conn.close()

    def stop(self):
        self.running = False
        self.node.stop()
        print(f"[Server {self.server_id}] Shutting down.")


if __name__ == "__main__":
    server_id = int(sys.argv[1])
    server = Server(server_id)
    server.start()

    while True:
        try:
            cmd = input()
            result = server.node.handle_user_command(cmd)
            if result == "exit":
                server.stop()
                break
        except EOFError:
            break
        except KeyboardInterrupt:
            server.stop()
            sys.exit(0)
