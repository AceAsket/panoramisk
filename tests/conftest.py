import os
from pathlib import Path
import subprocess
import time
import uuid

import pytest

from panoramisk import utils


class Asterisk:
    def __init__(self, compose, port):
        self.cwd = str(Path(__file__).resolve().parent / 'docker')
        self.command = compose + ['--project-name', 'panoramisk-test-' + uuid.uuid4().hex[:12]]
        self.env = dict(os.environ, PANORAMISK_AMI_PORT=str(port))
        self.port = port
        self.started = False

    def run(self, *args, timeout=30):
        result = subprocess.run(
            self.command + list(args), cwd=self.cwd, env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            encoding='utf8', errors='replace', timeout=timeout)
        if result.returncode:
            raise RuntimeError('Compose command failed: %s\n%s' % (' '.join(args), result.stdout))
        return result.stdout

    def start(self):
        self.stop()
        self.started = True
        self.run('up', '--detach', timeout=60)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            logs = self.logs()
            if 'Asterisk Ready.' in logs:
                return
            time.sleep(.1)
        raise RuntimeError('Asterisk did not become ready:\n' + self.logs())

    def logs(self, tail=20):
        return self.run('logs', '--no-color', '--tail=%s' % tail, 'asterisk')

    def stop(self):
        if self.started:
            try:
                self.run('down', '--volumes', '--timeout', '1')
            finally:
                self.started = False


@pytest.fixture
def asterisk(unused_tcp_port):
    compose = None
    for command in (['docker', 'compose'], ['docker-compose']):
        try:
            result = subprocess.run(command + ['version'], stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            compose = command
            break
    if compose is None:
        if os.environ.get('PANORAMISK_REQUIRE_DOCKER') == '1':
            pytest.fail('Docker Compose is required but not available')
        pytest.skip('Docker Compose is not available')
    original_eol = utils.EOL
    utils.EOL = '\r\n'
    server = Asterisk(compose, unused_tcp_port)
    try:
        yield server
    finally:
        try:
            server.stop()
        finally:
            utils.EOL = original_eol
