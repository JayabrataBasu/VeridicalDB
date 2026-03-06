"""Tests for connection startup/authentication message handling."""

import struct

from veridicaldb.connection import Connection
from veridicaldb.exceptions import DatabaseError
from veridicaldb.protocol import (
    MSG_AUTHENTICATION,
    MSG_BACKEND_KEY_DATA,
    MSG_ERROR_RESPONSE,
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

    def parse_error_response(self, _data):
        return {'M': 'auth failed'}


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

    def test_unsupported_authentication_method_raises(self):
        fake = _FakeProtocol([
            (MSG_AUTHENTICATION, struct.pack('>I', 10)),
        ])

        conn = Connection.__new__(Connection)
        conn.protocol = fake
        conn.password = ''
        conn._in_transaction = False

        try:
            conn._handle_authentication()
            assert False, "expected DatabaseError for unsupported authentication method"
        except DatabaseError as e:
            assert "Unsupported authentication method" in str(e)

    def test_ready_before_authentication_raises(self):
        fake = _FakeProtocol([
            (MSG_READY_FOR_QUERY, b'I'),
        ])

        conn = Connection.__new__(Connection)
        conn.protocol = fake
        conn.password = ''
        conn._in_transaction = False

        try:
            conn._handle_authentication()
            assert False, "expected DatabaseError when ReadyForQuery arrives before AuthenticationOk"
        except DatabaseError as e:
            assert "before authentication completed" in str(e)

    def test_error_response_during_authentication_raises(self):
        fake = _FakeProtocol([
            (MSG_ERROR_RESPONSE, b''),
        ])

        conn = Connection.__new__(Connection)
        conn.protocol = fake
        conn.password = ''
        conn._in_transaction = False

        try:
            conn._handle_authentication()
            assert False, "expected DatabaseError for authentication error response"
        except DatabaseError as e:
            assert "Authentication failed" in str(e)
