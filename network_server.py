import time
import threading

class NetworkServer:
    def __init__(self, all_servers):
        # all_servers is a dict: {node_id: (host, port)}
        self.all_servers = all_servers
        # link_states will store tuples (src, dest) as keys, True/False as values
        # True means link is active, False means link is failed.
        self.link_states = {}
        # node_states will store node_id: True/False
        # True means node is alive, False means node is failed.
        self.node_states = {}

        # Initialize all links as active and all nodes as alive.
        for src in all_servers:
            self.node_states[src] = True
            for dest in all_servers:
                if src != dest:
                    # ensure a consistent key ordering
                    edge = tuple(sorted((src, dest)))
                    self.link_states[edge] = True

    def fail_link(self, src, dest):
        edge = tuple(sorted((src, dest)))
        if edge in self.link_states:
            self.link_states[edge] = False
            print(f"Link between {src} and {dest} failed.")
        else:
            print(f"No link known between {src} and {dest}.")

    def fix_link(self, src, dest):
        edge = tuple(sorted((src, dest)))
        if edge in self.link_states:
            self.link_states[edge] = True
            print(f"Link between {src} and {dest} fixed.")
        else:
            print(f"No link known between {src} and {dest}.")

    def fail_node(self, node):
        if node in self.node_states:
            self.node_states[node] = False
            print(f"Node {node} failed.")
        else:
            print(f"No such node {node}.")

    def is_link_active(self, src, dest):
        # Check if both nodes are alive and link is active
        if not self.node_states.get(src, False):
            return False
        if not self.node_states.get(dest, False):
            return False
        edge = tuple(sorted((src, dest)))
        return self.link_states.get(edge, False)

    def is_node_alive(self, node):
        return self.node_states.get(node, False)

    def send_message(self, src, dest, data, send_func):
        # Check link
        if not self.is_link_active(src, dest):
            # If link is down or node dead, do nothing
            return

        def delayed_send():
            time.sleep(3)
            send_func(dest, data)
        t = threading.Thread(target=delayed_send)
        t.start()
