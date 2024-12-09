import sys
import json
import threading
import socket
import google.generativeai as genai

from kv_store import KeyValueStore
from paxos_node import PaxosNode
from network_server import NetworkServer

SERVER_ADDRESSES = {
    1: ('localhost', 5001),
    2: ('localhost', 5002),
    3: ('localhost', 5003)
}

class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.all_servers = SERVER_ADDRESSES
        self.kv_store = KeyValueStore()
        self.network_server = NetworkServer(self.all_servers)
        # pass a send_func callback to PaxosNode
        self.node = PaxosNode(server_id, self.all_servers, self.kv_store, self.network_server, self.send_func)
        self.running = True

    def send_func(self, dest, data):
        # Actually send data over the socket (no delay or checks here)
        if not self.network_server.is_node_alive(dest):
            # If node dead, don't send
            return
        addr = self.all_servers[dest]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect(addr)
            sock.sendall(data)
        except Exception as e:
            print(f"[Server {self.server_id}] Error sending message to {dest}: {e}")
        finally:
            sock.close()

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

    def handle_user_command(self, cmd):
        parts = cmd.strip().split()
        if len(parts) == 0:
            return None
        if parts[0] == "failLink":
            # failLink <src> <dest>
            if len(parts) < 3:
                print("Usage: failLink <src> <dest>")
                return None
            src = int(parts[1])
            dest = int(parts[2])
            self.network_server.fail_link(src, dest)
        elif parts[0] == "fixLink":
            # fixLink <src> <dest>
            if len(parts) < 3:
                print("Usage: fixLink <src> <dest>")
                return None
            src = int(parts[1])
            dest = int(parts[2])
            self.network_server.fix_link(src, dest)
        elif parts[0] == "failNode":
            # failNode <nodeNum>
            if len(parts) < 2:
                print("Usage: failNode <nodeNum>")
                return None
            node_num = int(parts[1])
            self.network_server.fail_node(node_num)
        else:
            return self.node.handle_user_command(cmd)
        return None

if __name__ == "__main__":
    # Load the config file
    with open("config.json", "r") as f:
        config = json.load(f)

    # Configure LLM using the key from config.json
    genai.configure(api_key=config["GEMINI_API_KEY"])

    server_id = int(sys.argv[1])
    server = Server(server_id)
    server.start()

    while True:
        try:
            cmd = input()
            result = server.handle_user_command(cmd)
            if result == "exit":
                server.stop()
                break
        except EOFError:
            break
        except KeyboardInterrupt:
            server.stop()
            sys.exit(0)
