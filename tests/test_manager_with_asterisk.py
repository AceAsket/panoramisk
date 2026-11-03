import asyncio

import pytest

from panoramisk import Manager


@pytest.mark.asyncio
@pytest.mark.parametrize('send_before_disconnect', [False, True])
async def test_reconnection_without_lost(event_loop, asterisk, send_before_disconnect):
    logged_in = event_loop.create_future()
    disconnected = event_loop.create_future()

    def on_login(manager):
        if not logged_in.done():
            logged_in.set_result(manager)

    def on_disconnect(manager, exc):
        if not disconnected.done():
            disconnected.set_result(exc)

    manager = Manager(loop=event_loop,
                      port=asterisk.port,
                      username='username',
                      secret='mysecret',
                      reconnect_timeout=1,
                      on_login=on_login,
                      on_disconnect=on_disconnect)
    try:
        await event_loop.run_in_executor(None, asterisk.start)
        await asyncio.wait_for(manager.connect(), 10)
        await asyncio.wait_for(logged_in, 10)
        response = await asyncio.wait_for(manager.send_action({'Action': 'Ping'}), 5)
        assert response.success
        protocol = manager.protocol

        if send_before_disconnect:
            # Keep connection_lost queued until the action is submitted, as in
            # the original regression test.
            asterisk.stop()
            action = manager.send_action({'Action': 'Status'})
            await asyncio.wait_for(disconnected, 10)
        else:
            await event_loop.run_in_executor(None, asterisk.stop)
            await asyncio.wait_for(disconnected, 10)
            action = manager.send_action({'Action': 'Status'})
        assert action in manager.awaiting_actions
        assert not action.done()

        logged_in = event_loop.create_future()
        await event_loop.run_in_executor(None, asterisk.start)
        await asyncio.wait_for(logged_in, 10)
        await asyncio.wait_for(action, 10)
        assert manager.protocol is not protocol
        assert not manager.awaiting_actions
        messages = [message async for message in action]
        assert messages
        assert messages[0].eventlist.lower() == 'start'
        assert messages[-1].eventlist.lower() == 'complete'
    finally:
        manager.close()
