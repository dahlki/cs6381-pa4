from threading import Lock

from kazoo.recipe.election import Election

from cs6381_zkclient import ZooClient

import cs6381_constants as constants


class LoadBalancer:
    # role=broker (role, election path), path=brokers, ip=10.0.0.4, port=6001
    def __init__(self, role, path, ip, port, min=1, max=3):
        self.zk = ZooClient(role, ip, port)
        self.zk_backup = ZooClient(f"{role}-replica", ip, port)

        self.election = None
        self.election_watcher = None
        self.watcher = None

        self.backup_election = None
        self.backup_election_watcher = None

        self.role = role
        self.election_path = role
        self.backup_election_path = f"{role}-replica"
        self.path = path

        self.ip = ip
        self.port = port
        self.address = f"{ip}:{port}"

        self.replicas = []
        self.primary_replicas = {}
        self.primary_replicas_path = "/primary-replicas"
        self.primary_replicas_data_path = "/primary-replicas/{}"

        self.min= min
        self.max = max

        self.setup(self.path, self.election_path, self.backup_callback, self.election_callback)

        self._lock = Lock()

    def setup(self, path, election_path, backup_callback, election_callback):
        self.election = self.zk.join_election()
        self.zk.register(path, f"{self.ip}:{self.port}")
        self.election_watcher = self.zk.get_watcher(election_path, election_callback)
        self.watcher = self.zk.get_watcher(path, backup_callback, True)

    def backup_callback(self, path, data):
        print(f"BACKUP watcher callback, path: {path} data: {data}")
        if data:
            for primary in self.primary_replicas:
                if primary not in data:
                    del self.primary_replicas[primary]

            self.replicas = data
            # if self.address in self.replicas:
                # self.backup_election = self.zk_backup.join_election()
                # self.backup_election_watcher = self.zk_backup.get_watcher(self.backup_election_path, self.election_callback)
        print(f"primary replicas: {self.primary_replicas}")
        print(f"backup replicas: {self.replicas}")
        print(f"self ip: {self.ip}, port: {self.port}")

    def election_callback(self, path, data):
        print(f"ELECTION watcher callback, path: {path} data: {data}")
        if data:
            elected_primary = data.decode()
            primary_replicas = self.zk.zk.get(self.primary_replicas_path) if self.zk.zk.exists(self.primary_replicas_path) else None
            primary_replica_data = self.zk.zk.get(self.primary_replicas_data_path.format(elected_primary)) if self.zk.zk.exists(self.primary_replicas_data_path.format(elected_primary)) else None
            print(f"PRIMARY_REPLICA DATA FROM ZK: {primary_replica_data}")
            print(f"PRIMARY_REPLICAS FROM ZK: {primary_replicas}")

            if primary_replica_data is None:
                self.zk.zk.create(self.primary_replicas_data_path.format(elected_primary), value="0".encode(), ephemeral=True, makepath=True)
            else:
                if elected_primary not in primary_replicas:
                    self.zk.zk.create(self.primary_replicas_path, value=self.address.encode(),
                                      ephemeral=True, makepath=True)

            if elected_primary in self.replicas:
                self.replicas.remove(elected_primary)

            print(f"primary: {elected_primary} == address: {self.address}")
            if elected_primary == self.address:
                if self.zk.zk.exists("/brokers"):
                    print("self.zk.zk.exists({}) : {}".format("/brokers", self.zk.zk.get_children("/brokers")))

                # Election(self.role, self.address).cancel()
            print(f"primary replicas: {self.zk.zk.get_children(self.primary_replicas_path)}")
            print(f"primary replicas: {self.zk.zk.get(self.primary_replicas_data_path.format(elected_primary))}")
            print(f"backup replicas: {self.replicas}")

    def update_primary_threshold(self, address, add=True):
        with self._lock:
            if address in self.primary_replicas:
                if add:
                    self.primary_replicas[address] += 1
                else:
                    self.primary_replicas[address] -= 1


