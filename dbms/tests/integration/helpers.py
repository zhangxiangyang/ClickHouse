import os
import os.path as p
import re
import subprocess
import shutil
import time

import docker

BASE_TESTS_DIR = p.dirname(__file__)

DOCKER_COMPOSE_TEMPLATE = '''
version: '2'
services:
    {name}:
        image: ubuntu:14.04
        user: '{uid}'
        volumes:
            - {binary_path}:/usr/bin/clickhouse:ro
            - {configs_dir}:/etc/clickhouse-server/
            - {db_dir}:/var/lib/clickhouse/
            - {logs_dir}:/var/log/clickhouse-server/
        entrypoint:
            -  /usr/bin/clickhouse
            -  --config-file=/etc/clickhouse-server/config.xml
            -  --log-file=/var/log/clickhouse-server/clickhouse-server.log
        depends_on: {depends_on}
'''

MACROS_CONFIG_TEMPLATE = '''
<yandex>
    <macros>
        <instance>{name}</instance>
    </macros>
</yandex>
'''

class ClickHouseInstance:
    def __init__(self, base_path, name, custom_configs, zookeeper_required=False):
        self.name = name

        self.src_dir = p.abspath(p.join(BASE_TESTS_DIR, '../../../'))
        self.build_dir = p.abspath(p.join(BASE_TESTS_DIR, '../../../build/'))

        self.custom_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_configs]

        self.zookeeper_required = zookeeper_required

        self.path = p.abspath(p.join(base_path, name))
        self.docker_compose_path = p.join(self.path, 'docker_compose.yml')

        self.docker_id = None
        self.ip_address = None

    def create_dir(self, destroy_dir=True):
        if destroy_dir:
            self.destroy_dir()

        os.mkdir(self.path)

        configs_dir = p.join(self.path, 'configs')
        os.mkdir(configs_dir)

        shutil.copy(p.join(self.src_dir, 'dbms/src/Server/config.xml'), configs_dir)
        shutil.copy(p.join(self.src_dir, 'dbms/src/Server/users.xml'), configs_dir)

        config_d_dir = p.join(configs_dir, 'config.d')
        os.mkdir(config_d_dir)

        shutil.copy(p.join(BASE_TESTS_DIR, 'common_instance_config.xml'), config_d_dir)

        with open(p.join(config_d_dir, 'macros.xml'), 'w') as macros_config:
            macros_config.write(MACROS_CONFIG_TEMPLATE.format(name=self.name))

        if self.zookeeper_required:
            shutil.copy(p.join(BASE_TESTS_DIR, 'zookeeper_config.xml'), config_d_dir)

        for path in self.custom_config_paths:
            shutil.copy(path, config_d_dir)

        db_dir = p.join(self.path, 'database')
        os.mkdir(db_dir)

        logs_dir = p.join(self.path, 'logs')
        os.mkdir(logs_dir)

        depends_on = '[]'
        if self.zookeeper_required:
            depends_on = '["zoo1", "zoo2", "zoo3"]'

        with open(self.docker_compose_path, 'w') as docker_compose:
            docker_compose.write(DOCKER_COMPOSE_TEMPLATE.format(
                name=self.name,
                uid=os.getuid(),
                binary_path=p.join(self.build_dir, 'dbms/src/Server/clickhouse'),
                configs_dir=configs_dir,
                config_d_dir=config_d_dir,
                db_dir=db_dir,
                logs_dir=logs_dir,
                depends_on=depends_on))

    def destroy_dir(self):
        if p.exists(self.path):
            shutil.rmtree(self.path)


class ClickHouseCluster:
    def __init__(self, base_path):
        self.base_dir = p.dirname(base_path)

        self.project_name = os.getlogin() + p.basename(self.base_dir)
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r'[^a-z0-9]', '', self.project_name.lower())

        self.base_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name', self.project_name]
        self.instances = {}
        self.with_zookeeper = False
        self.is_up = False

    def add_instance(self, name, custom_configs, zookeeper_required=False):
        if self.is_up:
            raise Exception('Can\'t add instance %s: cluster is already up!' % name)

        if name in self.instances:
            raise Exception('Can\'t add instance %s: there is already instance with the same name!' % name)

        instance = ClickHouseInstance(self.base_dir, name, custom_configs, zookeeper_required)
        self.instances[name] = instance
        self.base_cmd.extend(['--file', instance.docker_compose_path])
        if zookeeper_required and not self.with_zookeeper:
            self.with_zookeeper = True
            self.base_cmd.extend(['--file', p.join(BASE_TESTS_DIR, 'docker_compose_zookeeper.yml')])

        return self

    def up(self):
        if self.is_up:
            return

        for instance in self.instances.values():
            instance.create_dir()

        subprocess.check_call(self.base_cmd + ['up', '-d'])
        self.is_up = True

        docker_client = docker.from_env()
        for instance in self.instances.values():
            instance.docker_id = self.project_name + '_' + instance.name + '_1'
            container = docker_client.containers.get(instance.docker_id)
            instance.ip_address = container.attrs['NetworkSettings']['Networks'].values()[0]['IPAddress']

    def down(self):
        subprocess.check_call(self.base_cmd + ['down', '--volumes'])
        self.is_up = False

        for instance in self.instances.values():
            instance.docker_id = None
            instance.ip_address = None


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

        handle = self._docker_client.api.exec_create(self._container.id, cmd, **kwargs)
        output = self._docker_client.api.exec_start(handle).decode('utf8')
        exit_code = self._docker_client.api.exec_inspect(handle)['ExitCode']

        if exit_code != 0:
            print output
            raise subprocess.CalledProcessError(exit_code, cmd)

        return output
