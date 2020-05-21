"""NSQ protocol parser.

:see: https://nsq.io/clients/tcp_protocol_spec.html
"""
import abc
import struct
import zlib
from typing import Any, Optional, Tuple, Union

import snappy

from . import consts
from .consts import FrameType, NSQCommands
from .exceptions import ProtocolError
from ..utils import _convert_to_bytes

__all__ = (
    'Reader', 'DeflateReader', 'SnappyReader', 'NSQResponseSchema',
    'NSQMessageSchema', 'NSQErrorSchema'
)


class NSQResponseSchema:
    """NSQ Response schema"""
    body: bytes
    frame_type: FrameType

    def __init__(self, body: bytes, frame_type: Union[FrameType, int] = None):
        self.body = body
        self.frame_type = (
            frame_type
            if isinstance(frame_type, FrameType) or frame_type is None
            else FrameType(frame_type)
        )

    def __repr__(self):
        return '<NSQResponseSchema frame_type:{}, body:{}, is_ok:{}>'.format(
            self.frame_type, self.body, self.is_ok)

    def __bool__(self):
        return True

    @property
    def is_ok(self) -> bool:
        return self.body == NSQCommands.OK

    @property
    def is_heartbeat(self) -> bool:
        return self.body == b'_heartbeat_'

    @property
    def is_message(self) -> bool:
        return self.frame_type.is_message

    @property
    def is_response(self) -> bool:
        return self.frame_type.is_response

    @property
    def is_error(self) -> bool:
        return self.frame_type.is_error


class NSQMessageSchema(NSQResponseSchema):
    """NSQ Message schema"""
    timestamp: int = None
    attempts: int = None
    id: str = None

    def __init__(
            self, timestamp: int, attempts: int, id_: bytes, body: bytes,
            frame_type: Union[FrameType, int]):
        super().__init__(body, frame_type)
        self.timestamp = timestamp
        self.attempts = attempts
        self.id = id_.decode('utf-8')

    def __repr__(self):
        return (
            '<NSQMessageSchema frame_type:{}, body:{}, timestamp:{}, '
            'attempts:{}, id:{}>'
        ).format(
            self.frame_type, self.body, self.timestamp, self.attempts,
            self.id
        )


class NSQErrorSchema(NSQResponseSchema):
    """NSQ Error"""
    code: str

    def __init__(
            self, code: bytes, body: bytes,
            frame_type: Union[FrameType, int]):
        super().__init__(body, frame_type)
        self.code = code.decode('utf-8')

    def __repr__(self):
        return '<NSQErrorSchema frame_type:{}, body:{}, code:{}>'.format(
            self.frame_type, self.body, self.code)

    def __bool__(self):
        return False


class BaseReader(metaclass=abc.ABCMeta):
    @abc.abstractmethod   # pragma: no cover
    def feed(self, chunk):
        pass

    @abc.abstractmethod  # pragma: no cover
    def get(self):
        pass

    @abc.abstractmethod   # pragma: no cover
    def encode_command(self, cmd, *args, data=None):
        pass


class BaseCompressReader(BaseReader):
    def __init__(self):
        self._parser = Reader()

    @abc.abstractmethod  # pragma: no cover
    def compress(self, data):
        pass

    @abc.abstractmethod  # pragma: no cover
    def decompress(self, chunk):
        pass

    def feed(self, chunk):
        if not chunk:
            return
        uncompressed = self.decompress(chunk)
        uncompressed and self._parser.feed(uncompressed)

    def get(self):
        return self._parser.get()

    def encode_command(self, cmd, *args, data=None):
        cmd = self._parser.encode_command(cmd, *args, data=data)
        return self.compress(cmd)


class DeflateReader(BaseCompressReader):
    def __init__(self, buffer=None, level=6):
        super().__init__()
        wbits = -zlib.MAX_WBITS
        self._decompressor = zlib.decompressobj(wbits)
        self._compressor = zlib.compressobj(level, zlib.DEFLATED, wbits)
        self.buffer = buffer
        buffer and self.feed(buffer)

    def compress(self, data):
        chunk = self._compressor.compress(data)
        compressed = chunk + self._compressor.flush(zlib.Z_SYNC_FLUSH)
        return compressed

    def decompress(self, chunk):
        return self._decompressor.decompress(chunk)


class SnappyReader(BaseCompressReader):
    def __init__(self, buffer=None):
        super().__init__()
        self._decompressor = snappy.StreamDecompressor()
        self._compressor = snappy.StreamCompressor()
        self.buffer = buffer
        buffer and self.feed(buffer)

    def compress(self, data):
        return self._compressor.add_chunk(data, compress=True)

    def decompress(self, chunk):
        return self._decompressor.decompress(chunk)


class Reader(BaseReader):
    def __init__(self, buffer: bytes = None):
        self._buffer = bytearray()
        self._is_header = False
        self._payload_size = 0
        buffer and self.feed(buffer)

    @property
    def buffer(self) -> bytearray:
        return self._buffer

    def feed(self, chunk: bytes):
        """Put raw chunk of data obtained from connection to buffer.

        :param chunk: Raw input data.
        :type chunk: :class:`bytes`
        """
        chunk and self._buffer.extend(chunk)

    def get(self) -> Optional[
        Union[NSQResponseSchema, NSQErrorSchema, NSQMessageSchema]
    ]:
        """Get from buffer NSQ response

        :raises ProtocolError: On unexpected NSQ message's FrameType
        :returns: Depends of ``frame_type``, returns
            :class:`NSQResponse`, :class:`NSQError`,  or :class:`NSQMessage`
        """
        buffer_size = len(self._buffer)

        if not self._is_header and buffer_size >= consts.DATA_SIZE:
            size = struct.unpack('>l', self._buffer[:consts.DATA_SIZE])[0]
            self._payload_size = size
            self._is_header = True

        if (
                self._is_header
                and buffer_size >= consts.DATA_SIZE + self._payload_size
        ):
            start, end = consts.DATA_SIZE, consts.HEADER_SIZE
            frame_type = FrameType(
                struct.unpack('>l', self._buffer[start:end])[0])
            resp = self._parse_payload(frame_type, self._payload_size)

            self._buffer = self._buffer[start + self._payload_size:]
            self._is_header = False
            self._payload_size = 0

            return resp

    def _parse_payload(
            self, frame_type: FrameType, payload_size: int
    ) -> Union[NSQResponseSchema, NSQErrorSchema, NSQMessageSchema]:
        """Parse from buffer NSQ response

        :raises ProtocolError: On unexpected NSQ message's FrameType
        :returns: Depends of ``frame_type``, returns
            :class:`NSQResponse`, :class:`NSQError`,  or :class:`NSQMessage`
        """
        if frame_type == FrameType.RESPONSE:
            return NSQResponseSchema(
                self._unpack_response(payload_size), frame_type=frame_type)
        if frame_type == FrameType.ERROR:
            return NSQErrorSchema(
                *self._unpack_error(payload_size), frame_type=frame_type)
        if frame_type == FrameType.MESSAGE:
            return NSQMessageSchema(
                *self._unpack_message(payload_size), frame_type=frame_type)

        raise ProtocolError('Got unexpected FrameType: {}'.format(frame_type))

    def _unpack_response(self, payload_size: int) -> bytes:
        """Unpack the response from the buffer"""
        start = consts.HEADER_SIZE
        end = consts.DATA_SIZE + payload_size
        return bytes(self._buffer[start:end])

    def _unpack_error(self, payload_size: int) -> Tuple[bytes, bytes]:
        """Unpack the error from the buffer"""
        error = self._unpack_response(payload_size)
        code, msg = error.split(maxsplit=1)
        return code, msg

    def _unpack_message(self, payload_size: int) -> Tuple[
            int, int, bytes, bytes]:
        """Unpack the message from the buffer.

        :see: https://docs.python.org/3/library/struct.html
        :rtype: :class:`NSQMessageSchema`
        :returns: NSQ Message
        """
        start = consts.HEADER_SIZE
        end = consts.DATA_SIZE + payload_size
        msg_len = end - start - consts.MSG_HEADER
        fmt = '>qh16s{}s'.format(msg_len)
        return struct.unpack(fmt, self._buffer[start:end])

    def encode_command(self, cmd: str, *args, data: Any = None) -> bytes:
        """Encode command to bytes"""
        _cmd = _convert_to_bytes(cmd.upper().strip())
        _args = [_convert_to_bytes(a) for a in args]
        body_data, params_data = b'', b''

        if len(_args):
            params_data = b' ' + b' '.join(_args)

        if data and isinstance(data, (list, tuple)):
            data_encoded = [self._encode_body(part) for part in data]
            num_parts = len(data_encoded)
            payload = struct.pack('>l', num_parts) + b''.join(data_encoded)
            body_data = struct.pack('>l', len(payload)) + payload
        elif data:
            body_data = self._encode_body(data)

        return b''.join((_cmd, params_data, consts.NL, body_data))

    @staticmethod
    def _encode_body(data: Any) -> bytes:
        _data = _convert_to_bytes(data)
        result = struct.pack('>l', len(_data)) + _data
        return result
