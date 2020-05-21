from enum import Enum

NL = b'\n'
DATA_SIZE = 4
FRAME_SIZE = 4
HEADER_SIZE = DATA_SIZE + FRAME_SIZE

TIMESTAMP_SIZE = 8
ATTEMPTS_SIZE = 2
MSG_ID_SIZE = 16
MSG_HEADER = TIMESTAMP_SIZE + ATTEMPTS_SIZE + MSG_ID_SIZE


class ConnectionStatus(Enum):
    CLOSED = 0
    CLOSING = 0
    INIT = 1
    CONNECTED = 2
    SUBSCRIBED = 3
    RECONNECTING = 4

    @property
    def is_closed(self) -> bool:
        return self == self.CLOSED

    @property
    def is_closing(self) -> bool:
        return self == self.CLOSING

    @property
    def is_init(self) -> bool:
        return self == self.INIT

    @property
    def is_connected(self) -> bool:
        return self == self.CONNECTED

    @property
    def is_subscribed(self) -> bool:
        return self == self.SUBSCRIBED

    @property
    def is_reconnecting(self) -> bool:
        return self == self.RECONNECTING

    def __bool__(self):
        return not self.is_closed and not self.is_closing and not self.is_init


class FrameType(Enum):
    RESPONSE = 0
    ERROR = 1
    MESSAGE = 2

    @property
    def is_response(self) -> bool:
        return self == self.RESPONSE

    @property
    def is_error(self) -> bool:
        return self == self.ERROR

    @property
    def is_message(self) -> bool:
        return self == self.MESSAGE


class NSQCommands:
    MAGIC_V2 = b'  V2'
    OK = b'OK'
    BIN_OK = b'\x00\x00\x00\x06\x00\x00\x00\x00OK'
    IDENTIFY = b'IDENTIFY'
    NOP = b'NOP'
    FIN = b'FIN'
    REQ = b'REQ'
    TOUCH = b'TOUCH'
    RDY = b'RDY'
    MPUB = b'MPUB'
    CLS = b'CLS'
    AUTH = b'AUTH'
    SUB = b'SUB'
    PUB = b'PUB'
    DPUB = b'DPUB'
