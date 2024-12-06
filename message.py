import json
from enum import Enum, auto

class MessageType(Enum):
    PREPARE = auto()
    PROMISE = auto()
    ACCEPT = auto()
    ACCEPTED = auto()
    DECIDE = auto()
    FORWARD = auto()
    ACK = auto()
    REDIRECT = auto()

class Message:
    def __init__(self, msg_type, sender_id, recipient_id, payload=None):
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.recipient_id = recipient_id
        self.payload = payload or {}

    def to_json(self):
        return json.dumps({
            'msg_type': self.msg_type.name,
            'sender_id': self.sender_id,
            'recipient_id': self.recipient_id,
            'payload': self.payload
        })

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        msg_type = MessageType[obj['msg_type']]
        return Message(msg_type, obj['sender_id'], obj['recipient_id'], obj['payload'])
