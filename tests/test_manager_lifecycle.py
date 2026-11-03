import asyncio
from unittest.mock import Mock

import pytest

from panoramisk import Manager
from panoramisk import utils
from panoramisk.ami_protocol import AMIProtocol
from panoramisk.message import Message


async def drain_loop():
    for _ in range(10):
        await asyncio.sleep(0)


@pytest.fixture
def managers(event_loop):
    instances = []
    errors = []
    original_eol = utils.EOL
    utils.EOL = '\r\n'
    event_loop.set_exception_handler(lambda loop, context: errors.append(context))

    def create(**config):
        config.setdefault('reconnect_timeout', 0)
        instance = Manager(loop=event_loop, **config)
        instances.append(instance)
        return instance

    yield create

    for instance in instances:
        instance.close()
    event_loop.run_until_complete(drain_loop())
    utils.EOL = original_eol
    assert not errors, errors


def mock_connections(monkeypatch, loop, results):
    calls = Mock()

    async def create_connection(*args, **kwargs):
        calls(*args, **kwargs)
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(loop, 'create_connection', create_connection)
    return calls


def connection():
    transport = Mock()
    protocol = AMIProtocol()
    protocol.connection_made(transport)
    return transport, protocol


@pytest.mark.asyncio
async def test_initial_failure_does_not_leave_background_retries(event_loop, managers, monkeypatch):
    calls = mock_connections(monkeypatch, event_loop, iter([ConnectionRefusedError()] * 20))
    manager = managers()
    with pytest.raises(ConnectionRefusedError):
        await manager.connect()
    await drain_loop()
    assert calls.call_count == 1


@pytest.mark.asyncio
async def test_initial_failure_can_retry_when_requested(event_loop, managers, monkeypatch):
    transport, protocol = connection()
    calls = mock_connections(monkeypatch, event_loop, iter([
        ConnectionRefusedError(), (transport, protocol)]))
    manager = managers(reconnect_on_failure=True)
    with pytest.raises(ConnectionRefusedError):
        await manager.connect()
    await drain_loop()
    assert calls.call_count == 2
    assert manager.protocol is protocol
    assert not protocol.closed


def test_run_forever_retries_initial_failure(event_loop, managers, monkeypatch):
    calls = mock_connections(monkeypatch, event_loop, iter([
        ConnectionRefusedError(), connection()]))
    manager = managers()
    monkeypatch.setattr(manager, 'run_forever', Mock())
    manager.connect(run_forever=True)
    event_loop.run_until_complete(drain_loop())
    assert calls.call_count == 2


@pytest.mark.asyncio
async def test_connection_loss_retries_even_after_a_failed_reconnect(event_loop, managers, monkeypatch):
    first, second = connection(), connection()
    calls = mock_connections(monkeypatch, event_loop, iter([
        first, ConnectionRefusedError(), second]))
    disconnected = Mock()
    manager = managers(on_disconnect=disconnected)
    await manager.connect()
    first[1].connection_lost(OSError('connection lost'))
    await drain_loop()
    assert calls.call_count == 3
    assert manager.protocol is second[1]
    disconnected.assert_called_once()


def test_close_is_idempotent_and_terminal(event_loop, managers, monkeypatch):
    calls = mock_connections(monkeypatch, event_loop, iter([connection()]))
    manager = managers()
    manager.close()
    manager.close()
    with pytest.raises(RuntimeError, match='closed'):
        manager.connect()
    event_loop.run_until_complete(drain_loop())
    calls.assert_not_called()


@pytest.mark.asyncio
async def test_close_during_initial_retry_delay(event_loop, managers, monkeypatch):
    calls = mock_connections(monkeypatch, event_loop, iter([ConnectionRefusedError()] * 20))
    manager = managers(reconnect_on_failure=True)
    with pytest.raises(ConnectionRefusedError):
        await manager.connect()
    manager.close()
    await drain_loop()
    assert calls.call_count == 1


@pytest.mark.asyncio
async def test_close_during_reconnect_delay(event_loop, managers, monkeypatch):
    first = connection()
    calls = mock_connections(monkeypatch, event_loop, iter([first] * 20))
    manager = managers()
    await manager.connect()
    first[1].connection_lost(OSError())
    manager.close()
    await drain_loop()
    assert calls.call_count == 1
    assert manager.pinger is None
    assert manager.authenticated is False


@pytest.mark.asyncio
async def test_close_cancels_inflight_connection(event_loop, managers, monkeypatch):
    started = event_loop.create_future()
    cancelled = event_loop.create_future()

    async def create_connection(*args, **kwargs):
        started.set_result(None)
        try:
            await event_loop.create_future()
        except asyncio.CancelledError:
            cancelled.set_result(True)
            raise

    monkeypatch.setattr(event_loop, 'create_connection', create_connection)
    manager = managers(reconnect_on_failure=True)
    future = manager.connect()
    await started
    manager.close()
    with pytest.raises(asyncio.CancelledError):
        await future
    assert await cancelled
    await drain_loop()


@pytest.mark.asyncio
@pytest.mark.parametrize('suppress_cancellation', [False, True])
async def test_close_releases_a_connection_that_finishes_late(
        event_loop, managers, monkeypatch, suppress_cancellation):
    transport, protocol = connection()
    connected = Mock()
    logged_in = Mock()
    if suppress_cancellation:
        started = event_loop.create_future()

        async def create_connection(*args, **kwargs):
            started.set_result(None)
            try:
                await event_loop.create_future()
            except asyncio.CancelledError:
                return transport, protocol
    else:
        future = event_loop.create_future()
        future.set_result((transport, protocol))
        create_connection = Mock(return_value=future)
    monkeypatch.setattr(event_loop, 'create_connection', create_connection)
    manager = managers(username='user', secret='secret', on_connect=connected, on_login=logged_in)
    manager.connect()
    if suppress_cancellation:
        await started
    manager.close()
    await drain_loop()
    transport.close.assert_called_once()
    transport.write.assert_not_called()
    connected.assert_not_called()
    logged_in.assert_not_called()


@pytest.mark.asyncio
async def test_late_connection_error_does_not_reconnect(event_loop, managers, monkeypatch):
    future = event_loop.create_future()
    future.set_exception(ConnectionRefusedError())
    create = Mock(return_value=future)
    monkeypatch.setattr(event_loop, 'create_connection', create)
    manager = managers(reconnect_on_failure=True)
    manager.connect()
    manager.close()
    await drain_loop()
    create.assert_called_once()


@pytest.mark.asyncio
async def test_concurrent_connect_calls_share_one_attempt(event_loop, managers, monkeypatch):
    calls = mock_connections(monkeypatch, event_loop, iter([connection(), connection()]))
    manager = managers()
    first, second = manager.connect(), manager.connect()
    await asyncio.gather(first, second)
    assert first is second
    assert calls.call_count == 1


@pytest.mark.asyncio
async def test_pending_login_does_not_ping_and_success_starts_ping(event_loop, managers, monkeypatch):
    transport, protocol = connection()
    mock_connections(monkeypatch, event_loop, iter([(transport, protocol)]))
    logged_in = Mock()
    manager = managers(username='user', secret='secret', on_login=logged_in)
    await manager.connect()
    assert manager.pinger is None
    manager.ping()
    assert transport.write.call_count == 1  # Login only.
    manager.authenticated_future.set_result(Message({'Response': 'Success'}))
    await drain_loop()
    logged_in.assert_called_once_with(manager)
    assert manager.authenticated is True
    assert manager.pinger is not None
    manager.ping()
    assert b'Ping' in transport.write.call_args[0][0]


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['login', 'challenge', 'missing_challenge', 'md5_login', 'exception'])
async def test_authentication_failure_closes_without_retry_or_ping(
        event_loop, managers, monkeypatch, caplog, stage):
    transport, protocol = connection()
    calls = mock_connections(monkeypatch, event_loop, iter([(transport, protocol)] * 20))
    disconnected, logged_in = Mock(), Mock()
    manager = managers(host='ami.example', username='user', secret='private-secret',
                       auth_type='md5' if stage in ('challenge', 'missing_challenge', 'md5_login') else None,
                       reconnect_on_failure=True, on_disconnect=disconnected, on_login=logged_in)
    await manager.connect()
    if stage == 'md5_login':
        manager.auth_challenge_future.set_result(Message({'Response': 'Success', 'Challenge': '123'}))
        await drain_loop()
    future = (manager.auth_challenge_future if stage in ('challenge', 'missing_challenge')
              else manager.authenticated_future)
    if stage == 'exception':
        future.set_exception(OSError('authentication exchange failed'))
    else:
        future.set_result(Message({'Response': 'Success' if stage == 'missing_challenge' else 'Error'}))
    await drain_loop()
    manager.ping()
    assert manager.authenticated is False
    assert manager.pinger is None
    assert protocol.closed
    transport.close.assert_called_once()
    assert calls.call_count == 1
    assert all(b'Ping' not in args[0][0] for args in transport.write.call_args_list)
    logged_in.assert_not_called()
    disconnected.assert_called_once()
    assert isinstance(disconnected.call_args[0][1], PermissionError)
    assert 'ami.example:5038' in caplog.text
    assert 'user=user' in caplog.text
    assert 'private-secret' not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize('md5', [False, True])
async def test_close_cancels_pending_authentication(event_loop, managers, monkeypatch, md5):
    transport, protocol = connection()
    mock_connections(monkeypatch, event_loop, iter([(transport, protocol)]))
    manager = managers(username='user', secret='secret', auth_type='md5' if md5 else None)
    await manager.connect()
    future = manager.auth_challenge_future if md5 else manager.authenticated_future
    manager.close()
    await drain_loop()
    assert future.cancelled()
    assert transport.write.call_count == 1
    assert manager.pinger is None


@pytest.mark.asyncio
async def test_close_suppresses_queued_login_callback(event_loop, managers, monkeypatch):
    transport, protocol = connection()
    mock_connections(monkeypatch, event_loop, iter([(transport, protocol)]))
    logged_in = Mock()
    manager = managers(username='user', secret='secret', on_login=logged_in)
    await manager.connect()
    future = manager.authenticated_future
    future.set_result(Message({'Response': 'Success'}))
    manager.login(future)
    manager.close()
    await drain_loop()
    logged_in.assert_not_called()
    assert manager.authenticated is False
    assert manager.pinger is None


@pytest.mark.asyncio
async def test_close_consumes_pending_authentication_error(event_loop, managers, monkeypatch):
    mock_connections(monkeypatch, event_loop, iter([connection()]))
    manager = managers(username='user', secret='secret')
    await manager.connect()
    manager.authenticated_future.set_exception(OSError('authentication exchange failed'))
    manager.close()
    await drain_loop()


@pytest.mark.asyncio
async def test_failed_login_closes_real_tcp_connection(event_loop, managers, unused_tcp_port):
    received = []
    clients = []
    closed = event_loop.create_future()

    class RejectingAMI(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport
            self.buffer = b''
            clients.append(self)
            transport.write(b'Asterisk Call Manager/1.3\r\n')

        def data_received(self, data):
            self.buffer += data
            while b'\r\n\r\n' in self.buffer:
                request, self.buffer = self.buffer.split(b'\r\n\r\n', 1)
                headers = dict(line.split(b': ', 1) for line in request.split(b'\r\n'))
                received.append(headers[b'Action'])
                self.transport.write(b'Response: Error\r\nMessage: Authentication failed\r\nActionID: ' +
                                     headers[b'ActionID'] + b'\r\n\r\n')

        def connection_lost(self, exc):
            if not closed.done():
                closed.set_result(None)

    server = await event_loop.create_server(RejectingAMI, '127.0.0.1', unused_tcp_port)
    manager = managers(host='127.0.0.1', port=unused_tcp_port, username='user', secret='secret',
                       reconnect_on_failure=True)
    try:
        await manager.connect()
        await asyncio.wait_for(closed, 2)
        await drain_loop()
        assert received == [b'Login']
        assert len(clients) == 1
        assert manager.authenticated is False
        assert manager.pinger is None
    finally:
        manager.close()
        server.close()
        await server.wait_closed()
        for client in clients:
            client.transport.close()


def test_protocol_close_handles_reentrant_connection_lost():
    transport, protocol = connection()
    factory = Mock()
    protocol.factory = factory
    transport.close.side_effect = lambda: protocol.connection_lost(None)
    protocol.close()
    assert protocol.closed
    transport.close.assert_called_once()
    factory.connection_lost.assert_not_called()


def test_protocol_can_disconnect_before_factory_is_attached():
    transport, protocol = connection()
    protocol.connection_lost(None)
    assert protocol.closed
    transport.close.assert_called_once()
