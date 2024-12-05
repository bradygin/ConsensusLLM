import json
from enum import Enum, auto

class MessageType(Enum):
    PREPARE = auto()
    PROMISE = auto()
    ACCEPT = auto()
    ACCEPTED = auto()
    DECIDE = auto()
    ACK = auto()
    TIMEOUT = auto()
    FORWARD = auto()
    REDIRECT = auto()

class Message:
    def __init__(self, msg_type, sender_id, recipient_id, payload=None):
        self.msg_type = msg_type            # Type of message (from MessageType)
        self.sender_id = sender_id          # ID of the sender
        self.recipient_id = recipient_id    # ID of the recipient
        self.payload = payload or {}        # Dictionary containing message data

    def to_json(self):
        # Serialize the message to a JSON string
        return json.dumps({
            'msg_type': self.msg_type.name,
            'sender_id': self.sender_id,
            'recipient_id': self.recipient_id,
            'payload': self.payload
        })

    @staticmethod
    def from_json(json_str):
        # Deserialize the JSON string back to a Message object
        data = json.loads(json_str)
        msg_type = MessageType[data['msg_type']]
        sender_id = data['sender_id']
        recipient_id = data['recipient_id']
        payload = data['payload']
        return Message(msg_type, sender_id, recipient_id, payload)

    def __str__(self):
        return f"Message(type={self.msg_type}, sender={self.sender_id}, recipient={self.recipient_id}, payload={self.payload})"
