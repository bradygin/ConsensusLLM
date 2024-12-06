import sys
import socket
import threading
import argparse
from kv_store import KeyValueStore
from llm_interface import LLMInterface
from paxos import PaxosNode
from message import Message, MessageType

class ServerNode:
    def __init__(self, server_id, peers):
        self.server_id = str(server_id)
        self.peers = [str(p) for p in peers if str(p) != self.server_id]

        self.kv_store = KeyValueStore()
        self.llm = LLMInterface()

        self.paxos_node = PaxosNode(
            node_id=self.server_id,
            peers=self.peers,
            send_message_func=self.send_message,
            apply_operation_func=self.apply_operation
        )

        self.connections = {}
        self.listen_port = 8000 + int(self.server_id)
        self.running = True

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(('127.0.0.1', self.listen_port))
        self.server_sock.listen(5)

        threading.Thread(target=self.accept_connections, daemon=True).start()

        for p in self.peers:
            p_port = 8000 + int(p)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect(('127.0.0.1', p_port))
                self.connections[p] = s
                s.send(f"ID:{self.server_id}\n".encode('utf-8'))
                threading.Thread(target=self.receive_loop, args=(p,s), daemon=True).start()
            except:
                pass

    def accept_connections(self):
        while self.running:
            conn, addr = self.server_sock.accept()
            data = conn.recv(1024).decode('utf-8').strip()
            if data.startswith("ID:"):
                peer_id = data[3:]
                self.connections[peer_id] = conn
                threading.Thread(target=self.receive_loop, args=(peer_id, conn), daemon=True).start()

    def receive_loop(self, peer_id, sock):
        while self.running:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                lines = data.decode('utf-8').strip().split('\n')
                for line in lines:
                    if not line.strip():
                        continue
                    msg = Message.from_json(line.strip())
                    self.paxos_node.handle_message(msg)
            except:
                break
        if peer_id in self.connections:
            del self.connections[peer_id]

    def send_message(self, msg):
        if msg.recipient_id is None:
            return
        sock = self.connections.get(msg.recipient_id)
        if sock:
            sock.send((msg.to_json() + '\n').encode('utf-8'))

    def apply_operation(self, operation):
        action = operation.get('action')
        context_id = operation.get('context_id')
        if action == 'create_context':
            print(f"Decided create_context {context_id}")
            self.kv_store.create_context(context_id)
        elif action == 'add_query':
            query = operation.get('query')
            print(f"Decided add_query {context_id} {query}")
            self.kv_store.add_query(context_id, query)
            ctx = self.kv_store.get_context(context_id)
            ans = self.llm.generate_response(ctx)
            print("LLM Response:\n", ans)
        elif action == 'add_answer':
            answer = operation.get('answer')
            print(f"Decided add_answer {context_id} {answer}")
            self.kv_store.add_answer(context_id, answer)

    def run_cli(self):
        while True:
            try:
                cmd = input(f"Server {self.server_id}> ").strip().split()
                if not cmd:
                    continue
                if cmd[0] == 'create':
                    if len(cmd) != 2:
                        print("Usage: create <context_id>")
                        continue
                    op = {
                        'action': 'create_context',
                        'context_id': cmd[1]
                    }
                    self.paxos_node.propose(op)
                elif cmd[0] == 'query':
                    if len(cmd) < 3:
                        print("Usage: query <context_id> <query string>")
                        continue
                    context_id = cmd[1]
                    query_str = ' '.join(cmd[2:])
                    op = {
                        'action': 'add_query',
                        'context_id': context_id,
                        'query': query_str
                    }
                    self.paxos_node.propose(op)
                elif cmd[0] == 'choose':
                    if len(cmd) != 3:
                        print("Usage: choose <context_id> <response_number>")
                        continue
                    context_id = cmd[1]
                    answer_num = cmd[2]
                    op = {
                        'action': 'add_answer',
                        'context_id': context_id,
                        'answer': f"Selected Answer from server {answer_num}"
                    }
                    self.paxos_node.propose(op)
                elif cmd[0] == 'view':
                    if len(cmd) != 2:
                        print("Usage: view <context_id>")
                        continue
                    ctx = self.kv_store.get_context(cmd[1])
                    if ctx is None:
                        print(f"Context {cmd[1]} does not exist.")
                    else:
                        print(f"Context {cmd[1]}:")
                        print(ctx)
                elif cmd[0] == 'viewall':
                    all_ctx = self.kv_store.get_all_contexts()
                    print("All Contexts:")
                    for cid, ctext in all_ctx.items():
                        print(cid + ":")
                        print(ctext)
                elif cmd[0] == 'exit':
                    break
                else:
                    print("Unknown command.")
            except (EOFError, KeyboardInterrupt):
                break
        self.running = False
        self.server_sock.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, required=True)
    parser.add_argument('--peers', nargs='+', type=int, required=True)
    args = parser.parse_args()

    node = ServerNode(args.id, args.peers)

    node.run_cli()

if __name__ == "__main__":
    main()