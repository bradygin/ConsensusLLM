import asyncio
import sys
import os
import argparse
import threading
import json

from kv_store import KeyValueStore
from llm_interface import LLMInterface
from paxos import PaxosNode
from message import Message, MessageType

class ServerNode:
    def __init__(self, server_id, peers, network_host='127.0.0.1', network_port=8888):
        self.server_id = str(server_id)
        self.peers = [str(peer_id) for peer_id in peers if str(peer_id) != self.server_id]
        self.network_host = network_host
        self.network_port = network_port
        self.reader = None
        self.writer = None
        self.running = True

        # Load the API key from the config file
        api_key = self.load_api_key()

        self.kv_store = KeyValueStore()
        self.llm_interface = LLMInterface(api_key=api_key)

        # Initialize the Paxos node
        self.paxos_node = PaxosNode(
            node_id=self.server_id,
            peers=self.peers,
            send_message_func=self.send_message,
            apply_operation_func=self.apply_operation  # Pass the apply_operation method
        )

    def load_api_key(self):
        try:
            with open('config.json') as config_file:
                config = json.load(config_file)
            api_key = config.get("GEMINI_API_KEY")
            if not api_key:
                raise ValueError("GEMINI_API_KEY not found in config.json.")
            return api_key
        except FileNotFoundError:
            print("config.json file not found.")
            exit(1)
        except json.JSONDecodeError:
            print("Error decoding config.json. Ensure it is valid JSON.")
            exit(1)
        except Exception as e:
            print(f"Error: {e}")
            exit(1)

    async def start(self):
        # Connect to the network server
        try:
            self.reader, self.writer = await asyncio.open_connection(self.network_host, self.network_port)
            # Send our node ID to the network server
            self.writer.write(f"{self.server_id}\n".encode())
            await self.writer.drain()
            print(f"Connected to network server at {self.network_host}:{self.network_port}")
            # Start listening for messages
            asyncio.create_task(self.listen_for_messages())
            # Start the command loop
            await self.command_loop()
        except ConnectionRefusedError:
            print(f"Failed to connect to network server at {self.network_host}:{self.network_port}")
            self.running = False

    async def command_loop(self):
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                user_input = await loop.run_in_executor(None, input, f"Server {self.server_id}> ")
                user_input = user_input.strip()
                if not user_input:
                    continue
                await self.handle_command(user_input)
            except (KeyboardInterrupt, EOFError):
                print("\nShutting down server...")
                self.running = False
                self.writer.close()
                await self.writer.wait_closed()
                break

    async def handle_command(self, command_line):
        tokens = command_line.strip().split()
        if not tokens:
            return

        command = tokens[0]

        if command == "create":
            await self.handle_create(tokens[1:])
        elif command == "query":
            await self.handle_query(tokens[1:])
        elif command == "choose":
            await self.handle_choose(tokens[1:])
        elif command == "view":
            await self.handle_view(tokens[1:])
        elif command == "viewall":
            await self.handle_viewall()
        elif command == "exit":
            self.running = False
            self.writer.close()
            await self.writer.wait_closed()
        else:
            print(f"Unknown command: {command}")

    async def handle_create(self, args):
        if len(args) != 1:
            print("Usage: create <context ID>")
            return
        context_id = args[0]
        # Create the operation
        operation = {
            'action': 'create_context',
            'context_id': context_id,
            'originator': self.server_id
        }
        # Propose the operation via Paxos
        self.paxos_node.propose(operation)

    async def handle_query(self, args):
        if len(args) < 2:
            print("Usage: query <context ID> <query string>")
            return
        context_id = args[0]
        query_str = ' '.join(args[1:])
        # Create the operation
        operation = {
            'action': 'add_query',
            'context_id': context_id,
            'query': query_str,
            'originator': self.server_id
        }
        # Propose the operation via Paxos
        self.paxos_node.propose(operation)

    async def handle_choose(self, args):
        if len(args) != 2:
            print("Usage: choose <context ID> <response number>")
            return
        context_id = args[0]
        response_number = args[1]
        # In this implementation, response_number corresponds to the server ID
        selected_answer = self.response_options.get(response_number)
        if not selected_answer:
            print(f"No response available from server {response_number}.")
            return
        # Create the operation
        operation = {
            'action': 'add_answer',
            'context_id': context_id,
            'answer': selected_answer,
            'originator': self.server_id
        }
        # Propose the operation via Paxos
        self.paxos_node.propose(operation)

    async def handle_view(self, args):
        if len(args) != 1:
            print("Usage: view <context ID>")
            return
        context_id = args[0]
        context = self.kv_store.get_context(context_id)
        if context:
            print(f"Context {context_id}:")
            print(context)
        else:
            print(f"Context {context_id} does not exist.")

    async def handle_viewall(self):
        all_contexts = self.kv_store.get_all_contexts()
        if all_contexts:
            print("All Contexts:")
            for cid, ctx in all_contexts.items():
                print(f"{cid}:")
                print(ctx)
        else:
            print("No contexts available.")

    async def listen_for_messages(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break  # Connection closed
                message = data.decode().strip()
                # Message format: sender_id|message_data
                if '|' not in message:
                    continue
                sender_id, message_data = message.split('|', 1)
                # Deserialize the message
                msg = Message.from_json(message_data)
                # Pass the message to the Paxos node
                self.paxos_node.receive_message(msg)
        except asyncio.IncompleteReadError:
            pass
        finally:
            print("Disconnected from network server.")
            self.running = False

    def send_message(self, msg):
        # Serialize the message
        msg_json = msg.to_json()
        # Send the message to the recipient via the network server
        message = f"{msg.recipient_id}|{msg_json}\n"
        self.writer.write(message.encode())
        asyncio.create_task(self.writer.drain())

    def apply_operation(self, operation):
        action = operation.get('action')
        context_id = operation.get('context_id')
        originator = operation.get('originator')
        if action == 'create_context':
            self.kv_store.create_context(context_id)
        elif action == 'add_query':
            query = operation.get('query')
            self.kv_store.add_query(context_id, query)
            # If this node originated the query, generate LLM responses
            if originator == self.server_id:
                asyncio.create_task(self.generate_llm_responses(context_id))
        elif action == 'add_answer':
            answer = operation.get('answer')
            self.kv_store.add_answer(context_id, answer)
        else:
            print(f"Unknown operation: {action}")

    async def generate_llm_responses(self, context_id):
        # Retrieve the current context
        context = self.kv_store.get_context(context_id)
        if context is None:
            print(f"Context {context_id} does not exist.")
            return

        # Generate response using the LLM interface
        response = self.llm_interface.generate_response(context)
        if response:
            print("LLM Response:")
            print(response)
            # Store the response options (simulate responses from multiple servers)
            self.response_options = {self.server_id: response}
            print(f"Received responses from servers: {list(self.response_options.keys())}")
            print("Use 'choose <context ID> <response number>' to select the best answer.")
        else:
            print("Failed to get response from LLM.")

def main():
    parser = argparse.ArgumentParser(description="Start a server node.")
    parser.add_argument('--id', type=int, required=True, help='Server ID')
    parser.add_argument('--peers', nargs='+', type=int, required=True, help='List of peer node IDs')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Network server host')
    parser.add_argument('--port', type=int, default=8888, help='Network server port')
    args = parser.parse_args()

    server = ServerNode(
        server_id=args.id,
        peers=args.peers,
        network_host=args.host,
        network_port=args.port
    )

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\nServer shutting down.")

if __name__ == "__main__":
    main()