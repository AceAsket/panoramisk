Contribute
==========

Feel free to clone the project on `GitHub <https://github.com/gawel/panoramisk>`_.

Once you made a change, try to add a test for your feature/fix. At least assume
that you have'nt broke anything by running tox::

    $ tox
    ...
    py34: commands succeeded
    py35: commands succeeded
    flake8: commands succeeded
    docs: commands succeeded
    congratulations :)

 You can run tests for a specific version::

    $ tox -e py35

Running individual test::

    $ tox -e py35 tests/test_manager.py::test_connection


Asterisk integration test
-------------------------

Install the test dependencies and run the reconnect test with Docker Compose::

    $ python -m pip install -e '.[test]'
    $ docker compose -f tests/docker/docker-compose.yml pull
    $ python -m pytest tests/test_manager_with_asterisk.py -v

Set ``PANORAMISK_REQUIRE_DOCKER=1`` in the environment to fail if Compose is
unavailable; CI uses this setting so the integration test cannot silently skip.
The fixture creates a separate Compose project and a loopback AMI port for each
test, then removes its containers and network during cleanup.

To use a preloaded Asterisk image, set ``PANORAMISK_ASTERISK_IMAGE`` to its tag.
The image must provide ``/usr/sbin/asterisk``. The test mounts its own AMI and
module configuration and does not need SIP ports or external services.


You can also build the docs with::

    $ tox -e docs

And check the result::

    $ firefox .tox/docs/tmp/html/index.html
