:mod:`panoramisk` - AMI
========================

An API to communicate with Asterisk's AMI

Configure Asterisk
--------------------------

In ``/etc/asterisk/manager.conf``, add:

.. code-block:: ini

    [username]
    secret=password
    deny=0.0.0.0/0.0.0.0
    permit=127.0.0.1/255.255.255.255
    read = all
    write = all

Launch:

.. code-block:: sh

    $ rasterisk -x 'manager reload'


Connection lifetime
-------------------

``Manager.close()`` is permanent and idempotent. It cancels pending connection
attempts, reconnect timers, authentication and Ping. A connection that finishes
after ``close()`` is closed immediately. Create a new manager to connect again;
calling ``connect()`` on a closed manager raises ``RuntimeError``.

By default, an initial TCP connection failure is returned by ``await connect()``
without leaving a background retry loop. This lets a service's own watcher decide
when to create another manager. Always close a manager when discarding it:

.. code-block:: python

    manager = Manager(host=host, username=username, secret=secret)
    try:
        await manager.connect()
        # connect() waits for TCP; Login completes separately.
        response = await manager.authenticated_future
        if not response.success:
            raise PermissionError('AMI authentication failed')
    except BaseException:
        manager.close()
        raise

For a listener that should retry even when the server is unavailable at startup,
use ``Manager(reconnect_on_failure=True, ...)``. ``connect(run_forever=True)``
also enables initial retries. After a successful TCP connection, unexpected
connection loss always retries at ``reconnect_timeout`` intervals until closed.
Concurrent ``connect()`` calls share the same pending connection attempt.

Actions submitted while reconnecting are queued and sent after the new server
session emits ``FullyBooted``. Forgettable actions such as Ping and Login fail
with ``ConnectionError`` while disconnected. Permanent ``close()`` cancels
outstanding action futures and clears the queue; submitting new actions to a
closed manager raises ``ConnectionError``.

Ping starts after Login succeeds (or immediately if no username is configured).
A rejected Login or MD5 Challenge logs an error with the host, port and username,
closes the manager permanently, and calls ``on_disconnect(manager, exc)`` with a
``PermissionError``. It does not retry rejected credentials. A service maintaining
a manager registry should remove that manager in its disconnect callback before
creating a replacement. The ``on_connect`` callback means TCP is connected;
``on_login`` means authentication succeeded.

Library diagnostics use the ``panoramisk.manager`` logger. Applications with a
separate logging pipeline should pass their configured logger as ``log=logger``
or attach a handler to this logger to make the errors visible in that pipeline.
Passwords and MD5 keys are not included in authentication failure diagnostics.


API
---

.. automodule:: panoramisk

.. autoclass:: Manager
   :members:
