import errno
import subprocess as sp
from threading import Timer

class Client:
    def __init__(self, host, port=9000, command='clickhouse-client'):
        self.host = host
        self.port = port
        self.command = command

    def query(self, sql, timeout=10.0):
        process = sp.Popen(
            [self.command, '--multiquery', '--host', self.host, '--port', str(self.port)],
            stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)

        timer = None
        if timeout is not None:
            def kill_process():
                try:
                    process.kill()
                except OSError as e:
                    if e.errno != errno.ESRCH:
                        raise

            timer = Timer(timeout, kill_process)
            timer.start()

        stdout, stderr = process.communicate(sql)

        if timer is not None:
            if timer.finished.is_set():
                raise Exception('Query timed out!')
            else:
                timer.cancel()

        if process.returncode != 0:
            raise Exception('Query failed! Client return code: {}, stderr: {}'.format(process.returncode, stderr))

        return stdout
