"""Tests for connection startup/authentication message handling."""

import struct

from veridicaldb.connection import Connection
from veridicaldb.protocol import (
    MSG_AUTHENTICATION,
    MSG_BACKEND_KEY_DATA,
    MSG_PARAMETER_STATUS,
    MSG_READY_FOR_QUERY,
)


class _FakeProtocol:
    def __init__(self, messages):
        self._messages = list(messages)
        self.backend_pid = None
        self.backend_secret = None
        self.parameters = {}
        self.password_messages = 0

    def receive_message(self):
        return self._messages.pop(0)

    def parse_authentication(self, data):
        return struct.unpack('>I', data)[0]

    def send_password_message(self, _password):
        self.password_messages += 1

    def parse_backend_key_data(self, data):
        return struct.unpack('>II', data)

    def parse_parameter_status(self, data):
        key, value, _ = data.split(b'\x00', 2)
        return key.decode('utf-8'), value.decode('utf-8')

    def parse_ready_for_query(self, data):
        return chr(data[0])


class TestConnectionStartup:
    def test_authentication_drains_until_ready(self):
        fake = _FakeProtocol([
            (MSG_AUTHENTICATION, struct.pack('>I', 3)),
            (MSG_AUTHENTICATION, struct.pack('>I', 0)),
            (MSG_PARAMETER_STATUS, b'server_version\x00v2.0.0\x00'),
            (MSG_BACKEND_KEY_DATA, struct.pack('>II', 1234, 5678)),
            (MSG_READY_FOR_QUERY, b'I'),
        ])

        conn = Connection.__new__(Connection)
        conn.protocol = fake
        conn.password = ''
        conn._in_transaction = False

        conn._handle_authentication()

        assert fake.password_messages == 1
        assert fake.parameters['server_version'] == 'v2.0.0'
        assert fake.backend_pid == 1234
        assert fake.backend_secret == 5678
        assert conn.in_transaction is False
