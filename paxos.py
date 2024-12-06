import threading
import traceback
from message import Message, MessageType
from utils import generate_ballot_number

class PaxosNode:
    def __init__(self, node_id, peers, send_message_func, apply_operation_func):
        self.node_id = node_id
        self.peers = peers
        self.send_message = send_message_func
        self.apply_operation = apply_operation_func

        self.ballot_number = (0, int(self.node_id), 0)
        self.promised_ballot = None
        self.accepted_ballot = None
        self.accepted_value = None

        self.leader_id = None
        self.is_leader = False
        self.state_lock = threading.Lock()

        self.pending_proposals = []
        self.current_operation_number = 0
        self.received_promises = set()
        self.accepted_votes = set()

    def propose(self, operation):
        with self.state_lock:
            if not self.is_leader:
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
                    self.pending_proposals.append(operation)
                    self.start_election()
            else:
                self.pending_proposals.append(operation)
                self.start_accept_phase(operation)

    def start_election(self):
        self.ballot_number = generate_ballot_number(self.ballot_number, int(self.node_id))
        self.promised_ballot = self.ballot_number
        self.received_promises = set([self.node_id])
        print(f"PREPARE {self.ballot_number} from {self.node_id}")
        prepare_msg = Message(
            MessageType.PREPARE,
            sender_id=self.node_id,
            recipient_id=None,
            payload={'ballot_number': self.ballot_number}
        )
        self.broadcast_message(prepare_msg)

    def start_accept_phase(self, operation):
        print(f"ACCEPT {self.ballot_number} from {self.node_id} with operation {operation}")
        accept_msg = Message(
            MessageType.ACCEPT,
            sender_id=self.node_id,
            recipient_id=None,
            payload={
                'ballot_number': self.ballot_number,
                'operation': operation,
                'op_num': self.current_operation_number
            }
        )
        self.broadcast_message(accept_msg)

    def handle_prepare(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            print(f"Received PREPARE {incoming_ballot} from Server {msg.sender_id}")
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
                print(f"Sending PROMISE {incoming_ballot} to Server {msg.sender_id}")
                self.send_message(promise_msg)

    def handle_promise(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            print(f"Received PROMISE {incoming_ballot} from Server {msg.sender_id}")
            if incoming_ballot == self.ballot_number:
                self.received_promises.add(msg.sender_id)
                if len(self.received_promises) > (len(self.peers) + 1)//2:
                    self.is_leader = True
                    self.leader_id = self.node_id
                    print(f"Node {self.node_id} became the leader with ballot {self.ballot_number}.")
                    for op in self.pending_proposals:
                        self.start_accept_phase(op)

    def handle_accept(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            operation = msg.payload['operation']
            print(f"Received ACCEPT {incoming_ballot} {operation} from Server {msg.sender_id}")
            op_num = msg.payload['op_num']
            if (self.promised_ballot is None or incoming_ballot >= self.promised_ballot) and op_num >= self.current_operation_number:
                self.promised_ballot = incoming_ballot
                self.accepted_ballot = incoming_ballot
                self.accepted_value = operation
                accepted_msg = Message(
                    MessageType.ACCEPTED,
                    sender_id=self.node_id,
                    recipient_id=msg.sender_id,
                    payload={
                        'ballot_number': incoming_ballot,
                        'op_num': op_num
                    }
                )
                print(f"Sending ACCEPTED {incoming_ballot} {operation} to Server {msg.sender_id}")
                self.send_message(accepted_msg)

    def handle_accepted(self, msg):
        with self.state_lock:
            incoming_ballot = msg.payload['ballot_number']
            print(f"Received ACCEPTED {incoming_ballot} from Server {msg.sender_id}")
            op_num = msg.payload['op_num']
            if incoming_ballot == self.ballot_number and self.is_leader:
                self.accepted_votes.add(msg.sender_id)
                if len(self.accepted_votes) > (len(self.peers) + 1)//2:
                    decide_msg = Message(
                        MessageType.DECIDE,
                        sender_id=self.node_id,
                        recipient_id=None,
                        payload={
                            'operation': self.accepted_value,
                            'op_num': op_num
                        }
                    )
                    print(f"DECIDE {self.ballot_number} from {self.node_id} with operation {self.accepted_value}")
                    self.broadcast_message(decide_msg)
                    self.accepted_votes.clear()
                    self.current_operation_number = op_num + 1
                    self.pending_proposals = []

    def handle_decide(self, msg):
        with self.state_lock:
            operation = msg.payload['operation']
            print(f"Received DECIDE {operation} from Server {msg.sender_id}")
            self.apply_operation(operation)

    def handle_forward(self, msg):
        with self.state_lock:
            print(f"Received FORWARD from Server {msg.sender_id}")
            if self.is_leader:
                op = msg.payload['operation']
                self.pending_proposals.append(op)
                ack_msg = Message(MessageType.ACK, self.node_id, msg.sender_id)
                print("Sending ACK back to proposer")
                self.send_message(ack_msg)
                self.start_accept_phase(op)
            else:
                if self.leader_id:
                    redirect_msg = Message(
                        MessageType.REDIRECT,
                        sender_id=self.node_id,
                        recipient_id=msg.sender_id,
                        payload={'leader_id': self.leader_id}
                    )
                    print("Sending REDIRECT to proposer")
                    self.send_message(redirect_msg)

    def handle_message(self, msg):
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
            print("ACK received.")
        elif msg.msg_type == MessageType.REDIRECT:
            new_leader = msg.payload['leader_id']
            self.leader_id = new_leader
            print(f"Updated leader to {new_leader}")

    def broadcast_message(self, msg):
        for p in self.peers:
            m = Message(msg.msg_type, msg.sender_id, p, msg.payload)
            self.send_message(m)
