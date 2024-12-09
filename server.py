import sys
import json
import threading
import socket
import time
import google.generativeai as genai

from kv_store import KeyValueStore
from paxos_node import PaxosNode

# Known addresses of servers
SERVER_ADDRESSES = {
    1: ('localhost', 5001),
    2: ('localhost', 5002),
    3: ('localhost', 5003)
}

def network_server_request(cmd):
    """Send a command to the central network server and return the response as a dict."""
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

class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.all_servers = SERVER_ADDRESSES
        self.kv_store = KeyValueStore()
        
        # pass a send_func callback to PaxosNode
        self.node = PaxosNode(server_id, self.all_servers, self.kv_store, self.send_func)
        self.running = True

    def delayed_send(self, dest, data):
        # This function runs in a separate thread to introduce a 3-second delay before sending.
        time.sleep(3)
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

    def send_func(self, dest, data):
        """Check node/link states and send data with a 3-second delay."""
        # Check node and link states before sending
        resp_node = network_server_request(f"is_node_alive {dest}")
        if not resp_node.get("alive", False):
            # node dead, do nothing
            return

        resp_link = network_server_request(f"is_link_active {self.server_id} {dest}")
        if not resp_link.get("active", False):
            # link down, do nothing
            return

        # If we reach here, node and link are active
        # Introduce the 3-second delay before actually sending
        threading.Thread(target=self.delayed_send, args=(dest, data), daemon=True).start()

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
        if parts[0] in ["failLink", "fixLink", "failNode"]:
            # Forward these commands to the global network server
            resp = network_server_request(cmd)
            if "error" in resp:
                print(resp["error"])
            elif "status" in resp:
                print(resp["status"])
        else:
            # Other commands go to PaxosNode
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
