# import asyncio
# import sys

# class NetworkServer:
#     def __init__(self, port):
#         self.port = port
#         self.active_nodes = {}  # {node_id: writer}
#         self.link_status = {}   # {(src_id, dest_id): True/False}
#         self.node_status = {}   # {node_id: True/False}
#         self.lock = asyncio.Lock()

#     async def start_server(self):
#         server = await asyncio.start_server(self.handle_node_connection, '127.0.0.1', self.port)
#         print(f'Network server started on port {self.port}')
#         # Start the command loop in a separate task
#         asyncio.create_task(self.command_loop())
#         async with server:
#             await server.serve_forever()

#     async def handle_node_connection(self, reader, writer):
#         # Receive the node ID from the node
#         data = await reader.readline()
#         node_id = data.decode().strip()
#         peername = writer.get_extra_info('peername')
#         print(f'Node {node_id} connected from {peername}')
#         async with self.lock:
#             self.active_nodes[node_id] = (reader, writer)
#             self.node_status[node_id] = True  # Node is active
#             # Initialize link status with all other nodes
#             for other_node in self.active_nodes:
#                 if other_node != node_id:
#                     self.link_status[(node_id, other_node)] = True
#                     self.link_status[(other_node, node_id)] = True
#         # Start listening for messages from this node
#         asyncio.create_task(self.handle_node_messages(node_id, reader))

#     async def handle_node_messages(self, node_id, reader):
#         try:
#             while True:
#                 data = await reader.readline()
#                 if not data:
#                     break  # Connection closed
#                 message = data.decode().strip()
#                 print(f"Network server received message from {node_id}: {message}")
#                 # Message format: recipient_id|message_data
#                 if '|' not in message:
#                     continue
#                 recipient_id, message_data = message.split('|', 1)
#                 # Simulate message passing delay
#                 asyncio.create_task(self.forward_message_with_delay(node_id, recipient_id, message_data))
#         except asyncio.IncompleteReadError:
#             pass
#         finally:
#             await self.disconnect_node(node_id)

#     async def forward_message_with_delay(self, sender_id, recipient_id, message_data):
#         await asyncio.sleep(3)  # 3-second delay
#         async with self.lock:
#             # Check if the link is active
#             link_key = (sender_id, recipient_id)
#             if not self.link_status.get(link_key, True):
#                 print(f"Message from {sender_id} to {recipient_id} dropped due to failed link.")
#                 return
#             # Check if the recipient node is active
#             if not self.node_status.get(recipient_id, False):
#                 print(f"Message from {sender_id} to {recipient_id} dropped because recipient is down.")
#                 return
#             recipient_reader_writer = self.active_nodes.get(recipient_id)
#             if recipient_reader_writer:
#                 _, recipient_writer = recipient_reader_writer
#                 # Send the message to the recipient
#                 recipient_writer.write(f"{sender_id}|{message_data}\n".encode())
#                 await recipient_writer.drain()
#                 print(f"Message from {sender_id} to {recipient_id} forwarded.")
#             else:
#                 print(f"Recipient {recipient_id} not found.")

#     async def disconnect_node(self, node_id):
#         async with self.lock:
#             print(f"Node {node_id} disconnected.")
#             self.active_nodes.pop(node_id, None)
#             self.node_status[node_id] = False
#             # Remove all links associated with this node
#             links_to_remove = [link for link in self.link_status if node_id in link]
#             for link in links_to_remove:
#                 self.link_status.pop(link, None)

#     async def command_loop(self):
#         while True:
#             command_line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
#             if not command_line:
#                 continue
#             command_line = command_line.strip()
#             tokens = command_line.split()
#             if not tokens:
#                 continue
#             command = tokens[0]
#             if command == "failLink" and len(tokens) == 3:
#                 await self.fail_link(tokens[1], tokens[2])
#             elif command == "fixLink" and len(tokens) == 3:
#                 await self.fix_link(tokens[1], tokens[2])
#             elif command == "failNode" and len(tokens) == 2:
#                 await self.fail_node(tokens[1])
#             elif command == "exit":
#                 print("Shutting down network server.")
#                 for node_id in list(self.active_nodes.keys()):
#                     await self.disconnect_node(node_id)
#                 break
#             else:
#                 print("Unknown command or incorrect parameters.")

#     async def fail_link(self, src_id, dest_id):
#         async with self.lock:
#             # Fail the link in both directions
#             self.link_status[(src_id, dest_id)] = False
#             self.link_status[(dest_id, src_id)] = False
#             print(f"Link between {src_id} and {dest_id} failed.")

#     async def fix_link(self, src_id, dest_id):
#         async with self.lock:
#             # Fix the link in both directions
#             self.link_status[(src_id, dest_id)] = True
#             self.link_status[(dest_id, src_id)] = True
#             print(f"Link between {src_id} and {dest_id} fixed.")

#     async def fail_node(self, node_id):
#         async with self.lock:
#             # Disconnect the node
#             node_reader_writer = self.active_nodes.get(node_id)
#             if node_reader_writer:
#                 _, writer = node_reader_writer
#                 writer.close()
#                 await writer.wait_closed()
#                 await self.disconnect_node(node_id)
#                 print(f"Node {node_id} failed.")
#             else:
#                 print(f"Node {node_id} not found.")

# def main():
#     import argparse

#     parser = argparse.ArgumentParser(description="Start the network server.")
#     parser.add_argument('--port', type=int, default=8888, help='Port number for the network server.')
#     args = parser.parse_args()

#     network_server = NetworkServer(port=args.port)
#     try:
#         asyncio.run(network_server.start_server())
#     except KeyboardInterrupt:
#         print("\nNetwork server shutting down.")

# if __name__ == "__main__":
#     main()
