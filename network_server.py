import socket
import threading
import json

class CentralNetworkServer:
    def __init__(self, all_servers):
        self.all_servers = all_servers
        self.link_states = {}
        self.node_states = {}

        # Initialize states
        for src in all_servers:
            self.node_states[src] = True
            for dest in all_servers:
                if src != dest:
                    edge = tuple(sorted((src, dest)))
                    self.link_states[edge] = True

    def handle_command(self, cmd_str):
        parts = cmd_str.strip().split()
        if len(parts) == 0:
            return {"error": "No command given"}

        cmd = parts[0]
        if cmd == "failNode":
            if len(parts) < 2:
                return {"error": "Usage: failNode <nodeNum>"}
            node = int(parts[1])
            if node in self.node_states:
                self.node_states[node] = False
                return {"status": f"Node {node} failed."}
            else:
                return {"error": f"No such node {node}"}

        elif cmd == "failLink":
            if len(parts) < 3:
                return {"error": "Usage: failLink <src> <dest>"}
            src = int(parts[1])
            dest = int(parts[2])
            edge = tuple(sorted((src, dest)))
            if edge in self.link_states:
                self.link_states[edge] = False
                return {"status": f"Link between {src} and {dest} failed."}
            else:
                return {"error": f"No link known between {src} and {dest}."}

        elif cmd == "fixLink":
            if len(parts) < 3:
                return {"error": "Usage: fixLink <src> <dest>"}
            src = int(parts[1])
            dest = int(parts[2])
            edge = tuple(sorted((src, dest)))
            if edge in self.link_states:
                self.link_states[edge] = True
                return {"status": f"Link between {src} and {dest} fixed."}
            else:
                return {"error": f"No link known between {src} and {dest}."}

        elif cmd == "is_node_alive":
            if len(parts) < 2:
                return {"error": "Usage: is_node_alive <node>"}
            node = int(parts[1])
            alive = self.node_states.get(node, False)
            return {"alive": alive}

        elif cmd == "is_link_active":
            if len(parts) < 3:
                return {"error": "Usage: is_link_active <src> <dest>"}
            src = int(parts[1])
            dest = int(parts[2])
            edge = tuple(sorted((src, dest)))
            alive = (self.node_states.get(src, False) and 
                     self.node_states.get(dest, False) and 
                     self.link_states.get(edge, False))
            return {"active": alive}

        else:
            return {"error": "Unknown command"}

    def start(self, host='localhost', port=6000):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.listen()
        print(f"[NetworkServer] Listening on {(host, port)} ...")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=self.client_handler, args=(conn, addr)).start()

    def client_handler(self, conn, addr):
        data = conn.recv(4096)
        if not data:
            conn.close()
            return
        cmd_str = data.decode('utf-8')
        response = self.handle_command(cmd_str)
        conn.sendall(json.dumps(response).encode('utf-8'))
        conn.close()

if __name__ == "__main__":
    # Hard-coded server addresses known here too
    SERVER_ADDRESSES = {
        1: ('localhost', 5001),
        2: ('localhost', 5002),
        3: ('localhost', 5003)
    }
    network_server = CentralNetworkServer(SERVER_ADDRESSES)
    network_server.start()
