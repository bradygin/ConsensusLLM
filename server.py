import socket
import threading
import sys
import json
import time

# Hard-code the server addresses for now. In a real scenario, we might pass these in or discover them dynamically.
SERVER_ADDRESSES = {
    1: ('localhost', 5001),
    2: ('localhost', 5002),
    3: ('localhost', 5003)
}

class PaxosNode:
    def __init__(self, server_id, all_servers):
        self.server_id = server_id
        self.all_servers = all_servers

        # Paxos state placeholders
        # ballot number structure: (seq_num, pid, op_num)
        self.current_ballot = (0, self.server_id, 0)
        self.promised_ballot = None
        self.accepted_value = None
        self.accepted_ballot = None

        # Leader and proposer tracking
        self.is_leader = False
        self.known_leader = None

        # Operation queue (for leader)
        self.operation_queue = []

    def handle_message(self, msg):
        """
        Handle incoming Paxos messages and respond accordingly.
        msg is expected to be a dictionary with fields like:
        {
          "type": "PREPARE" or "PROMISE" or "ACCEPT" or "ACCEPTED" or "DECIDE",
          "ballot": (seq_num, pid, op_num),
          "operation": ... # depends on message
        }
        """
        mtype = msg.get("type")

        if mtype == "PREPARE":
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
        
        # Check if we can promise
        # For now, let's just promise if this ballot is higher than what we've promised before.
        if self.promised_ballot is None or incoming_ballot > self.promised_ballot:
            self.promised_ballot = incoming_ballot
            # Send PROMISE back
            response = {
                "type": "PROMISE",
                "from": self.server_id,
                "to": msg["from"],
                "ballot": incoming_ballot,
                "accepted_ballot": self.accepted_ballot,
                "accepted_value": self.accepted_value
            }
            self.send_message(response)
            print(f"[Server {self.server_id}] Sending PROMISE {incoming_ballot} to Server {msg['from']}")
        else:
            # ignore prepare if ballot not greater
            pass

    def on_promise(self, msg):
        print(f"[Server {self.server_id}] Received PROMISE {msg['ballot']} from Server {msg['from']}")
        # If this server is trying to become leader, it collects promises.
        # For now, let's store them and once we have a majority, we send ACCEPT.
        # We’ll store them in a dictionary keyed by ballot.

        # Create a structure if not present
        if not hasattr(self, 'promise_responses'):
            self.promise_responses = {}
        bkey = tuple(msg["ballot"])
        if bkey not in self.promise_responses:
            self.promise_responses[bkey] = []
        self.promise_responses[bkey].append(msg)

        # Check if we have a majority
        # With 3 servers, majority = 2
        if len(self.promise_responses[bkey]) >= 2:
            # Become leader
            self.is_leader = True
            self.known_leader = self.server_id
            # Now send ACCEPT for the operation (for now let's just say we want to "createContext 1")
            accept_msg = {
                "type": "ACCEPT",
                "from": self.server_id,
                "to": "ALL",
                "ballot": bkey,
                "operation": "createContext 1"
            }
            print(f"[Server {self.server_id}] Sending ACCEPT {bkey} createContext 1 to ALL")
            self.broadcast_message(accept_msg)

    def on_accept(self, msg):
        print(f"[Server {self.server_id}] Received ACCEPT {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        incoming_ballot = tuple(msg["ballot"])
        # Check if we can accept this
        if self.promised_ballot is None or incoming_ballot >= self.promised_ballot:
            self.accepted_ballot = incoming_ballot
            self.accepted_value = msg["operation"]
            # Send ACCEPTED
            accepted_msg = {
                "type": "ACCEPTED",
                "from": self.server_id,
                "to": msg["from"],
                "ballot": incoming_ballot,
                "operation": msg["operation"]
            }
            print(f"[Server {self.server_id}] Sending ACCEPTED {incoming_ballot} {msg['operation']} to Server {msg['from']}")
            self.send_message(accepted_msg)
        else:
            # Ignore if we promised a higher ballot already
            pass

    def on_accepted(self, msg):
        print(f"[Server {self.server_id}] Received ACCEPTED {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        # If leader and we have majority accepted, send DECIDE
        # Similar to PROMISE logic, we track ACCEPTED responses
        if not hasattr(self, 'accepted_responses'):
            self.accepted_responses = {}
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

    def on_decide(self, msg):
        print(f"[Server {self.server_id}] Received DECIDE {msg['ballot']} {msg['operation']} from Server {msg['from']}")
        # Apply operation locally
        # For now, just print that we created the context
        op = msg["operation"]
        if op.startswith("createContext"):
            context_id = op.split()[1]
            print(f"[Server {self.server_id}] Context {context_id} created!")
        # Later, we’ll store this in the key-value store.

    def broadcast_message(self, msg):
        for sid, addr in self.all_servers.items():
            if sid != self.server_id:
                self.send_message(msg, sid)

    def send_message(self, msg, sid=None):
        if sid is None:
            # if sid not specified, try 'to' field in msg
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
        try:
            sock.connect(self.all_servers[sid])
            sock.sendall(data)
        except Exception as e:
            print(f"[Server {self.server_id}] Error sending message to {sid}: {e}")
        finally:
            sock.close()

    def start_leader_election(self):
        # Just send a PREPARE to try becoming leader
        ballot = (1, self.server_id, 0)
        prepare_msg = {
            "type": "PREPARE",
            "from": self.server_id,
            "to": "ALL",
            "ballot": ballot
        }
        print(f"[Server {self.server_id}] Sending PREPARE {ballot} to ALL")
        self.broadcast_message(prepare_msg)


class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.all_servers = SERVER_ADDRESSES
        self.node = PaxosNode(server_id, self.all_servers)
        self.running = True

    def start(self):
        # Start server socket
        t = threading.Thread(target=self.listen)
        t.start()

    def listen(self):
        # TCP server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(self.all_servers[self.server_id])
            s.listen()
            print(f"[Server {self.server_id}] Listening on {self.all_servers[self.server_id]} ...")
            while self.running:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

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


if __name__ == "__main__":
    server_id = int(sys.argv[1])
    server = Server(server_id)
    server.start()

    time.sleep(2)

    # If this is server 3, start the leader election process after a short wait
    if server_id == 3:
        time.sleep(1)
        server.node.start_leader_election()

    # Keep the server running
    while True:
        cmd = input().strip()
        if cmd == "exit":
            server.stop()
            break
