import asyncio
import hashlib
import logging
from collections import defaultdict
from collections import deque
import re
import fnmatch
from .ami_protocol import AMIProtocol
from . import actions
from . import utils


class Manager:
    """Main object:

    .. code-block:: python

        >>> manager = Manager(
        ...    host='127.0.0.1',
        ...    port=5038,
        ...    ssl=False,
        ...    encoding='utf8')
    """

    defaults = dict(
        host='127.0.0.1',
        port=5038,
        events='on',
        ssl=False,
        encoding='utf8',
        ping_delay=10,
        ping_interval=10,
        reconnect_timeout=2,
        reconnect_on_failure=False,
        protocol_factory=AMIProtocol,
        save_stream=None,
        loop=None,
        forgetable_actions=('ping', 'login'),
    )

    def __init__(self, **config):
        self.config = dict(self.defaults, **config)
        self.loop = self.config['loop']
        self.log = config.get('log', logging.getLogger(__name__))
        self.callbacks = defaultdict(list)
        self.protocol = None
        self.patterns = []
        self.save_stream = self.config.get('save_stream')
        self.authenticated = False
        self.authenticated_future = None
        self.awaiting_actions = deque()
        self.forgetable_actions = self.config['forgetable_actions']
        self.pinger = None
        self.ping_delay = int(self.config['ping_delay'])
        self.ping_interval = int(self.config['ping_interval'])
        self.reconnect_timeout = int(self.config['reconnect_timeout'])
        self._connected = False
        self._closed = False
        self._ever_connected = False
        self._connect_future = None
        self._reconnect_handle = None
        self._reconnect_on_failure = self.config['reconnect_on_failure']
        self.auth_type = self.config.get('auth_type')
        self.auth_challenge_future = None
        self.register_event('FullyBooted', self.send_awaiting_actions)
        self.on_login = config.get('on_login', on_login)
        self.on_connect = config.get('on_connect', on_connect)
        self.on_disconnect = config.get('on_disconnect', on_disconnect)

    def connection_made(self, f):
        if f is self._connect_future:
            self._connect_future = None
        if f.cancelled():
            return
        try:
            transport, protocol = f.result()
        except OSError as exc:
            if self._closed:
                return
            self._connected = False
            self.log.warning('Not able to connect to %s:%s: %s',
                             self.config['host'], self.config['port'], exc)
            if self._ever_connected or self._reconnect_on_failure:
                self._schedule_reconnect()
        else:
            if self._closed:
                protocol.close()
                return
            self._cancel_authentication()
            self._stop_pinger()
            if self.protocol is not None and self.protocol is not protocol:
                self.protocol.close()
            self._connected = True
            self._ever_connected = True
            self.authenticated = False
            self.log.debug('Manager connected')
            self.protocol = protocol
            self.protocol.queue = deque()
            self.protocol.factory = self
            self.protocol.log = self.log
            self.protocol.config = self.config
            self.protocol.encoding = self.encoding = self.config['encoding']
            self.responses = self.protocol.responses = {}
            self.loop.call_soon(self._run_callback, self.on_connect, protocol)
            if 'username' in self.config:
                if self.auth_type is not None:
                    if self.auth_type.lower() == 'md5':
                        self.auth_challenge_future = self.send_action({
                            'Action': 'Challenge',
                            'AuthType': self.auth_type.upper()})
                        self.auth_challenge_future.add_done_callback(self.secure_login)
                        return
                self.authenticated = False
                self.authenticated_future = self.send_action({
                    'Action': 'Login',
                    'Username': self.config['username'],
                    'Secret': self.config['secret'],
                    'Events': self.config['events']})
                self.authenticated_future.add_done_callback(self.login)
            else:
                self.log.debug('username not in config file')
                self.pinger = self.loop.call_later(self.ping_delay, self.ping)

    def _run_callback(self, callback, protocol):
        if not self._closed and self._connected and protocol is self.protocol:
            callback(self)

    def _schedule_reconnect(self):
        if not self._closed and self._reconnect_handle is None:
            self._reconnect_handle = self.loop.call_later(
                self.reconnect_timeout, self._reconnect)

    def _reconnect(self):
        self._reconnect_handle = None
        if not self._closed:
            self.connect()

    def _stop_pinger(self):
        if self.pinger is not None:
            self.pinger.cancel()
            self.pinger = None

    def _cancel_authentication(self):
        for name, callback in (('authenticated_future', self.login),
                               ('auth_challenge_future', self.secure_login)):
            future = getattr(self, name)
            if future is not None:
                future.remove_done_callback(callback)
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    future.exception()
                setattr(self, name, None)

    def _authentication_failed(self, stage):
        exc = PermissionError('AMI %s failed for %s:%s (user=%s)' % (
            stage, self.config['host'], self.config['port'],
            self.config.get('username', '')))
        self.log.error('%s', exc)
        self.close()
        self.loop.call_soon(self.on_disconnect, self, exc)

    def secure_login(self, future):
        if self._closed or future.cancelled():
            return
        self.auth_challenge_future = None
        try:
            resp = future.result()
        except Exception:
            self._authentication_failed('MD5 challenge')
            return
        if resp.success and resp.Challenge:
            auth_challenge = resp.Challenge + self.config['secret']
            key = hashlib.md5(auth_challenge.encode('utf-8')).hexdigest()
            self.authenticated = False
            self.authenticated_future = self.send_action({
                'Action': 'Login',
                'Username': self.config['username'],
                'AuthType': self.auth_type.upper(),
                'Key': key,
                'Events': self.config['events']})
            self.authenticated_future.add_done_callback(self.login)
        else:
            self._authentication_failed('MD5 challenge')

    def login(self, future):
        if self._closed or future.cancelled():
            return False
        self.authenticated_future = None
        try:
            resp = future.result()
        except Exception:
            self._authentication_failed('login')
            return False
        self.authenticated = bool(resp.success)
        self._stop_pinger()
        if self.authenticated:
            self.loop.call_soon(self._run_callback, self.on_login, self.protocol)
            self.pinger = self.loop.call_later(self.ping_delay, self.ping)
        else:
            self._authentication_failed('login')
        return self.authenticated

    def ping(self):
        self._stop_pinger()
        if (self._closed or not self._connected or
                ('username' in self.config and not self.authenticated)):
            return
        self.pinger = self.loop.call_later(self.ping_interval, self.ping)
        self.protocol.send({'Action': 'Ping'})

    async def send_awaiting_actions(self, *_):
        self.log.info('Sending awaiting actions')
        while (self.awaiting_actions and self._connected and not self._closed
               and not self.protocol.closed):
            action = self.awaiting_actions.popleft()
            if action['action'].lower() not in self.forgetable_actions:
                if not action.done():
                    self.send_action(action, as_list=action.as_list)

    def send_action(self, action, as_list=None, **kwargs):
        """Send an :class:`~panoramisk.actions.Action` to the server:

        :param action: an Action or dict with action name and parameters to
                       send
        :type action: Action or dict or Command
        :param as_list: If True, the action will retrieve all responses
        :type as_list: boolean
        :return: an Action that will receive the response(s)
        :rtype: panoramisk.actions.Action

        :Example:

        To retrieve answer::

            manager = Manager()
            resp = await manager.send_action({'Action': 'Status'})

        Or with an async for::

            manager = Manager()
            async for resp in  manager.send_action({'Action': 'Status'}):
                print(resp)

        See https://wiki.asterisk.org/wiki/display/AST/AMI+Actions for
        more information on actions
        """
        if self._closed:
            raise ConnectionError('Manager is closed')
        action.update(kwargs)
        return self.protocol.send(action, as_list=as_list)

    def send_command(self, command, as_list=False):
        """Send a :class:`~panoramisk.actions.Command` to the server::

            manager = Manager()
            resp = await manager.send_command('http show status')

        Return a response :class:`~panoramisk.message.Message`.
        See https://wiki.asterisk.org/wiki/display/AST/ManagerAction_Command
        """
        action = actions.Action({'Command': command, 'Action': 'Command'},
                                as_list=as_list)
        return self.send_action(action)

    def send_agi_command(self, channel, command, as_list=False):
        """Send a :class:`~panoramisk.actions.Command` to the server:

        :param channel: Channel name where to launch command.
               Ex: 'SIP/000000-00000a53'
        :type channel: String
        :param command: command to launch. Ex: 'GET VARIABLE async_agi_server'
        :type command: String
        :param as_list: If True, the action Future will retrieve all responses
        :type as_list: boolean
        :return: a Future that will receive the response
        :rtype: asyncio.Future

        :Example:

        ::

            manager = Manager()
            resp = manager.send_agi_command('SIP/000000-00000a53',
                                            'GET VARIABLE async_agi_server')


        Return a response :class:`~panoramisk.message.Message`.
        See https://wiki.asterisk.org/wiki/display/AST/Asterisk+11+ManagerAction_AGI
        """
        action = actions.Command({'Action': 'AGI',
                                  'Channel': channel,
                                  'Command': command},
                                 as_list=as_list)
        return self.send_action(action)

    def connect(self, run_forever=False, on_startup=None, on_shutdown=None):
        """Connect to the server. A closed manager cannot be reused.

        Initial failures are returned to the caller without background retries,
        unless ``reconnect_on_failure=True`` or ``run_forever=True`` is used.
        Once connected, unexpected connection loss is retried automatically.
        """
        if self._closed:
            raise RuntimeError('Manager is closed; create a new Manager to connect')
        if self.loop is None:  # pragma: no cover
            self.loop = asyncio.get_event_loop()
        if run_forever:
            self._reconnect_on_failure = True
        if self._reconnect_handle is not None:
            self._reconnect_handle.cancel()
            self._reconnect_handle = None
        if self._connect_future is None:
            self._connect_future = asyncio.ensure_future(
                self.loop.create_connection(
                    self.config['protocol_factory'],
                    self.config['host'], self.config['port'],
                    ssl=self.config['ssl']), loop=self.loop)
            self._connect_future.add_done_callback(self.connection_made)
        t = self._connect_future

        if run_forever:
            self.run_forever(on_startup, on_shutdown)
        return t

    def run_forever(self, on_startup, on_shutdown):
        """Start loop forever"""
        try:
            if on_startup:
                self.loop.run_until_complete(on_startup(self))
            self.loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            self.close()
        finally:
            if on_shutdown:
                self.loop.run_until_complete(on_shutdown(self))
            self.loop.stop()

    def register_event(self, pattern, callback=None):
        """register an event. See :class:`~panoramisk.message.Message`:

        .. code-block:: python

            >>> def callback(manager, event):
            ...     print(manager, event)
            >>> manager = Manager()
            >>> manager.register_event('Meetme*', callback)
            <function callback at 0x...>

        You can also use the manager as a decorator:

        .. code-block:: python

            >>> manager = Manager()
            >>> @manager.register_event('Meetme*')
            ... def callback(manager, event):
            ...     print(manager, event)
        """
        def _register_event(callback):
            if not self.callbacks[pattern]:
                self.patterns.append((pattern,
                                     re.compile(fnmatch.translate(pattern))))
            self.callbacks[pattern].append(callback)
            return callback
        if callback is not None:
            return _register_event(callback)
        else:
            return _register_event

    def dispatch(self, event):
        matches = []
        event.manager = self
        for pattern, regexp in self.patterns:
            match = regexp.match(event.event)
            if match is not None:
                matches.append(pattern)
                for callback in self.callbacks[pattern]:
                    ret = callback(self, event)
                    if (asyncio.iscoroutine(ret) or
                            isinstance(ret, asyncio.Future)):
                        asyncio.ensure_future(ret, loop=self.loop)
        return matches

    def close(self):
        """Permanently close the manager and cancel connection attempts."""
        self._closed = True
        self._connected = False
        self.authenticated = False
        if self._reconnect_handle is not None:
            self._reconnect_handle.cancel()
            self._reconnect_handle = None
        if self._connect_future is not None and not self._connect_future.done():
            self._connect_future.cancel()
        self._stop_pinger()
        self._cancel_authentication()
        if getattr(self, 'protocol', None):
            for action in list(self.protocol.responses.values()):
                if not action.done():
                    action.cancel()
            self.protocol.close()
        while self.awaiting_actions:
            action = self.awaiting_actions.popleft()
            if not action.done():
                action.cancel()

    def connection_lost(self, exc):
        if self._closed:
            return
        self._connected = False
        self.authenticated = False
        self._cancel_authentication()
        self._stop_pinger()
        self.log.error('Connection lost from %s:%s',
                       self.config['host'], self.config['port'])
        self.loop.call_soon(self.on_disconnect, self, exc)
        self.log.info('Try to connect again in %d second(s)' % self.reconnect_timeout)
        self._schedule_reconnect()

    @classmethod
    def from_config(cls, filename_or_fd, section='asterisk', **kwargs):
        config = utils.config(filename_or_fd, section=section)
        config.update(kwargs)
        return cls(**config)


# noinspection PyUnusedLocal
def on_connect(manager: Manager):
    """
    Callback after connect
    """
    pass


# noinspection PyUnusedLocal
def on_login(manager: Manager):
    """
    Callback after login
    """
    pass


# noinspection PyUnusedLocal
def on_disconnect(manager: Manager, exc: Exception):
    """
    Callback after disconnect
    """
    pass
