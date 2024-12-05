import threading
import traceback
from message import Message, MessageType
from utils import generate_ballot_number

class PaxosNode:
    def __init__(self, node_id, peers, send_message_func, apply_operation_func):
        self.node_id = node_id          # Unique ID of this node (as a string)
        self.peers = peers              # List of peer node IDs (as strings)
        self.send_message = send_message_func  # Function to send messages to other nodes
        self.apply_operation = apply_operation_func  # Callback to apply decided operations

        # Paxos state variables
        self.ballot_number = (0, int(self.node_id), 0)  # (seq_num, pid, op_num)
        self.promised_ballot = None
        self.accepted_ballot = None
        self.accepted_value = None

        self.leader_id = None
        self.is_leader = False
        self.state_lock = threading.Lock()

        # For Multi-Paxos
        self.log = []  # List of decided values
        self.pending_proposals = []
        self.current_operation_number = 0

        # For message handling
        self.message_queue = []
        self.message_condition = threading.Condition()

        # Start the message handling thread
        threading.Thread(target=self.message_handler_thread, daemon=True).start()

    def propose(self, operation):
        with self.state_lock:
            if not self.is_leader:
                # Forward the operation to the leader
                if self.leader_id:
                    msg = Message(
                        MessageType.FORWARD,
                        sender_id=self.node_id,
                        recipient_id=self.leader_id,
                        payload={'operation': operation}
                    )
                    self.send_message(msg)
                    print(f"Forwarded operation to leader {self.leader_id}")
                else:
                    print("No known leader. Becoming proposer.")
                    self.start_election()
                    self.pending_proposals.append(operation)
            else:
                # Add operation to pending proposals
                self.pending_proposals.append(operation)
                # Start accept phase
                self.start_accept_phase(operation)

    def start_election(self):
        with self.state_lock:
            self.ballot_number = generate_ballot_number(self.ballot_number, int(self.node_id))
            self.promised_ballot = self.ballot_number
            self.vote_count = 1  # Vote for self
            self.received_promises = set()
            prepare_msg = Message(
                MessageType.PREPARE,
                sender_id=self.node_id,
                recipient_id=None,  # Broadcast
                payload={'ballot_number': self.ballot_number}
            )
            self.broadcast_message(prepare_msg)
            print(f"Sent PREPARE with ballot {self.ballot_number}")

    def start_accept_phase(self, operation):
        with self.state_lock:
            accept_msg = Message(
                MessageType.ACCEPT,
                sender_id=self.node_id,
                recipient_id=None,  # Broadcast
                payload={
                    'ballot_number': self.ballot_number,
                    'operation': operation,
                    'op_num': self.current_operation_number
                }
            )
            self.broadcast_message(accept_msg)
            print(f"Sent ACCEPT with ballot {self.ballot_number} and operation {operation}")

    def handle_prepare(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            if self.promised_ballot is None or incoming_ballot > self.promised_ballot:
                self.promised_ballot = incoming_ballot
                promise_msg = Message(
                    MessageType.PROMISE,
                    sender_id=self.node_id,
                    recipient_id=msg.sender_id,
                    payload={
                        'ballot_number': incoming_ballot,
                        'accepted_ballot': self.accepted_ballot,
                        'accepted_value': self.accepted_value
                    }
                )
                self.send_message(promise_msg)
                print(f"Sent PROMISE to {msg.sender_id} with ballot {incoming_ballot}")
            else:
                print(f"Ignored PREPARE from {msg.sender_id} with ballot {incoming_ballot}")

    def handle_promise(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            if incoming_ballot == self.ballot_number:
                self.received_promises.add(msg.sender_id)
                if len(self.received_promises) > (len(self.peers) + 1) // 2:
                    self.is_leader = True
                    self.leader_id = self.node_id
                    print(f"Node {self.node_id} became the leader.")
                    # Propose pending operations
                    for operation in self.pending_proposals:
                        self.start_accept_phase(operation)
            else:
                print(f"Received outdated PROMISE from {msg.sender_id}")

    def handle_accept(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            if self.promised_ballot is None or incoming_ballot >= self.promised_ballot:
                self.promised_ballot = incoming_ballot
                self.accepted_ballot = incoming_ballot
                self.accepted_value = msg.payload['operation']
                accepted_msg = Message(
                    MessageType.ACCEPTED,
                    sender_id=self.node_id,
                    recipient_id=msg.sender_id,
                    payload={
                        'ballot_number': incoming_ballot,
                        'op_num': msg.payload['op_num']
                    }
                )
                self.send_message(accepted_msg)
                print(f"Sent ACCEPTED to {msg.sender_id} with ballot {incoming_ballot}")
            else:
                print(f"Ignored ACCEPT from {msg.sender_id} with ballot {incoming_ballot}")

    def handle_accepted(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            if incoming_ballot == self.ballot_number:
                if not hasattr(self, 'accepted_votes'):
                    self.accepted_votes = set()
                self.accepted_votes.add(msg.sender_id)
                if len(self.accepted_votes) > (len(self.peers) + 1) // 2:
                    # Decision reached
                    decide_msg = Message(
                        MessageType.DECIDE,
                        sender_id=self.node_id,
                        recipient_id=None,  # Broadcast
                        payload={
                            'operation': self.accepted_value,
                            'op_num': self.current_operation_number
                        }
                    )
                    self.broadcast_message(decide_msg)
                    print(f"Broadcasted DECIDE with operation {self.accepted_value}")
                    self.current_operation_number += 1
                    self.accepted_votes.clear()
            else:
                print(f"Ignored ACCEPTED from {msg.sender_id} with ballot {incoming_ballot}")

    def handle_decide(self, msg):
        with self.state_lock:
            operation = msg.payload['operation']
            op_num = msg.payload['op_num']
            # Apply the operation
            self.log.append((op_num, operation))
            print(f"Applied operation {operation} at op_num {op_num}")
            # Apply operation to the key-value store
            self.apply_operation(operation)

    def handle_forward(self, msg):
        with self.state_lock:
            if self.is_leader:
                operation = msg.payload['operation']
                self.pending_proposals.append(operation)
                self.start_accept_phase(operation)
                ack_msg = Message(
                    MessageType.ACK,
                    sender_id=self.node_id,
                    recipient_id=msg.sender_id
                )
                self.send_message(ack_msg)
                print(f"ACK sent to {msg.sender_id} for forwarded operation")
            else:
                print(f"Received FORWARD but not the leader")
                # Optionally, inform the sender of the current leader
                if self.leader_id:
                    redirect_msg = Message(
                        MessageType.REDIRECT,
                        sender_id=self.node_id,
                        recipient_id=msg.sender_id,
                        payload={'leader_id': self.leader_id}
                    )
                    self.send_message(redirect_msg)

    def receive_message(self, msg):
        with self.message_condition:
            self.message_queue.append(msg)
            self.message_condition.notify()

    def message_handler_thread(self):
        while True:
            with self.message_condition:
                while not self.message_queue:
                    self.message_condition.wait()
                msg = self.message_queue.pop(0)
            try:
                self.process_message(msg)
            except Exception as e:
                print(f"Exception in message_handler_thread: {e}")
                traceback.print_exc()


    def process_message(self, msg):
        if msg.msg_type == MessageType.PREPARE:
            self.handle_prepare(msg)
        elif msg.msg_type == MessageType.PROMISE:
            self.handle_promise(msg)
        elif msg.msg_type == MessageType.ACCEPT:
            self.handle_accept(msg)
        elif msg.msg_type == MessageType.ACCEPTED:
            self.handle_accepted(msg)
        elif msg.msg_type == MessageType.DECIDE:
            self.handle_decide(msg)
        elif msg.msg_type == MessageType.FORWARD:
            self.handle_forward(msg)
        elif msg.msg_type == MessageType.ACK:
            # Handle ACK if needed
            print(f"Received ACK from {msg.sender_id}")
        elif msg.msg_type == MessageType.REDIRECT:
            # Update known leader
            new_leader_id = msg.payload['leader_id']
            with self.state_lock:
                self.leader_id = new_leader_id
            print(f"Updated leader to {new_leader_id}")
        else:
            print(f"Unknown message type: {msg.msg_type}")

    def broadcast_message(self, msg):
        # Send the message to all peers
        for peer_id in self.peers:
            msg_copy = Message(
                msg_type=msg.msg_type,
                sender_id=msg.sender_id,
                recipient_id=peer_id,
                payload=msg.payload
            )
            self.send_message(msg_copy)

    def get_log(self):
        with self.state_lock:
            return list(self.log)
