import os.path as p
import subprocess
import time

import docker

from .cluster import BASE_TESTS_DIR


# We need to call iptables to create partitions, but we want to avoid sudo.
# The way to circumvent this restriction is to run iptables in a container with network=host.
# The container is long-running and periodically renewed - this is an optimization to avoid the overhead
# of container creation on each call.
# Source of the idea: https://github.com/worstcase/blockade/blob/master/blockade/host.py

class NetworkManager:
    _instance = None

    @classmethod
    def get(cls, **kwargs):
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance

    def add_iptables_rule(self, **kwargs):
        cmd = ['iptables', '-A', 'DOCKER']
        cmd.extend(self._iptables_cmd_suffix(**kwargs))
        self._exec_run(cmd, privileged=True)

    def delete_iptables_rule(self, **kwargs):
        cmd = ['iptables', '-D', 'DOCKER']
        cmd.extend(self._iptables_cmd_suffix(**kwargs))
        self._exec_run(cmd, privileged=True)

    @staticmethod
    def _iptables_cmd_suffix(
            source=None, destination=None,
            source_port=None, destination_port=None,
            target=None):
        ret = []
        if source is not None:
            ret.extend(['-s', source])
        if destination is not None:
            ret.extend(['-d', destination])
        if source_port is not None:
            ret.extend(['-p', 'tcp', '--sport', str(source_port)])
        if destination_port is not None:
            ret.extend(['-p', 'tcp', '--dport', str(destination_port)])
        if target is not None:
            ret.extend(['-j', target])
        return ret


    def __init__(
            self,
            image_name='clickhouse_tests_helper',
            image_path=p.join(BASE_TESTS_DIR, 'helper_container'),
            container_expire_timeout=50, container_exit_timeout=60):

        self.container_expire_timeout = container_expire_timeout
        self.container_exit_timeout = container_exit_timeout

        self._docker_client = docker.from_env()

        try:
            self._image = self._docker_client.images.get(image_name)
        except docker.errors.ImageNotFound:
            self._image = self._docker_client.images.build(tag=image_name, path=image_path, rm=True)

        self._container = None

        self._ensure_container()

    def _ensure_container(self):
        if self._container is None or self._container_expire_time <= time.time():

            if self._container is not None:
                try:
                    self._container.remove(force=True)
                except docker.errors.NotFound:
                    pass

            # Work around https://github.com/docker/docker-py/issues/1477
            host_config = self._docker_client.api.create_host_config(network_mode='host', auto_remove=True)
            container_id = self._docker_client.api.create_container(
                self._image.id, command=('sleep %s' % self.container_exit_timeout),
                detach=True, host_config=host_config)['Id']

            self._container_expire_time = time.time() + self.container_expire_timeout
            self._docker_client.api.start(container_id)
            self._container = self._docker_client.containers.get(container_id)

        return self._container

    def _exec_run(self, cmd, **kwargs):
        container = self._ensure_container()

        handle = self._docker_client.api.exec_create(container.id, cmd, **kwargs)
        output = self._docker_client.api.exec_start(handle).decode('utf8')
        exit_code = self._docker_client.api.exec_inspect(handle)['ExitCode']

        if exit_code != 0:
            print output
            raise subprocess.CalledProcessError(exit_code, cmd)

        return output
