import sys
import json
import threading
import socket
import google.generativeai as genai

from kv_store import KeyValueStore
from paxos_node import PaxosNode

# Hard-coded server addresses for demonstration
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
            result = server.node.handle_user_command(cmd)
            if result == "exit":
                server.stop()
                break
        except EOFError:
            break
        except KeyboardInterrupt:
            server.stop()
            sys.exit(0)
