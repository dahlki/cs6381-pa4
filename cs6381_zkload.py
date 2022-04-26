from threading import Lock

from kazoo.recipe.election import Election
from cs6381_zkelection import Election as ELectionClient

from cs6381_zkclient import ZooClient

import cs6381_constants as constants


class LoadBalancer:
    # role=broker (role, election path), path=brokers, ip=10.0.0.4, port=6001
    def __init__(self, role, path, ip, port, replica_count=1, min=1, max=3):

        self.role_replicas_path = f"/{role}-replicas"
        self.role_replicas_data_path = self.role_replicas_path + "/{}"

        self.zk = ZooClient(role, ip, port)
        self.zk_replica = ZooClient(self.role_replicas_path[1:], ip, port)

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

        self.replica_count = replica_count
        self.current_count = 1

        self.replica_count_list = []
        self.primary_replicas = {}

        self.primary_replicas_path = "/primary-replicas"
        self.primary_replicas_data_path = "/primary-replicas/{}"

        self.min= min
        self.max = max

        self.setup(self.backup_election_path, self.election_path, self.backup_callback, self.election_callback)

        self._lock = Lock()

        self.setup_replica_count_list()

    def setup_replica_count_list(self):
        print("SILVIA SILVIA SILVIA SILVIA SILVIA SILVIA")

        print (self.current_count)
        print(f"replica count: {self.replica_count}")
        self.replica_count_list = list(range(1, self.replica_count + 1))
        print(f"replica count list: {self.replica_count_list}")
        print(f"replica count list length: {len(self.replica_count_list)}")

        if self.zk.zk.exists("/replica"):
            current_count_value = self.zk.zk.get("/replica")
            print(current_count_value)
            self.current_count = int(current_count_value[0].decode())
            self.current_count = self.current_count + 1 if self.current_count + 1 <= len(self.replica_count_list) else 1
            self.zk.zk.set("/replica", value=str(self.current_count).encode())

        else:
            self.zk.zk.create("/replica", value=str(self.current_count).encode(), ephemeral=True, makepath=True)

        print(f"replica current count: {self.current_count}")
        print(f"current replica count from ZK: ", self.zk.zk.get("/replica"))
        self.setup_election(self.current_count)

        self.create_replica_nodes()

    def setup_election(self, num):
        election_path = self.role_replicas_data_path.format(num)[1:]
        print("SETTING UP ELECTION FOR : {}".format(election_path))
        ELectionClient(self.zk_replica.zk, election_path, self.ip, self.port).register()

    def create_replica_nodes(self):
        # self.zk.zk.delete(f"{self.role_replicas_path}", recursive=True)
        # self.zk.zk.delete(self.role_replicas_data_path.format(self.current_count), recursive=True)
        print("\n***************in create_replica_nodes***************")
        print(f"\nBROKER ELECTION NUM: {self.current_count}\n")
        if not self.zk_replica.zk.exists(self.role_replicas_path):
            self.zk_replica.zk.create(self.role_replicas_path)

        self.setup_load(self.role_replicas_data_path.format(self.current_count), self.role_replicas_data_path.format(self.current_count), self.backup_callback, self.election_callback)
        self.zk_replica.get_watcher(self.role_replicas_path[1:], self.backup_callback, True)

    def setup_load(self, path, election_path, backup_callback, election_callback):
        print("in setup_load!!!!!!!")
        print(path)
        print(election_path)
        self.zk_replica.join_election()
        self.zk_replica.register(path, f"{self.ip}:{self.port}")
        self.election_watcher = self.zk_replica.get_watcher(election_path, election_callback)
        self.watcher = self.zk_replica.get_watcher(path, backup_callback, True)

    def setup(self, path, election_path, backup_callback, election_callback):
        self.election = self.zk.join_election()
        self.zk.register(path, f"{self.ip}:{self.port}")
        self.election_watcher = self.zk.get_watcher(election_path, election_callback)
        self.watcher = self.zk.get_watcher(path, backup_callback, False)

    def backup_callback(self, path, data):
        print(f"BACKUP watcher callback, path: {path} data: {data}")
        # if data:
        #     for primary in self.primary_replicas:
        #         if primary not in data:
        #             del self.primary_replicas[primary]
        #
        #     self.replicas = data
        print(f"self primary replicas: {self.primary_replicas}")
        print(f"self backup replicas: {self.replicas}")

    def election_callback(self, path, data):
        print(f"ELECTION watcher callback, path: {path} data: {data}")
        if data:
            elected_primary = data.decode()
            primary_replicas = self.zk.zk.get_children(self.primary_replicas_path) if self.zk.zk.exists(self.primary_replicas_path) else None
            print(f"PRIMARY_REPLICAS FROM ZK: {primary_replicas}")
            
            if not primary_replicas:
                self.zk.zk.create(f"{self.primary_replicas_path}/{elected_primary}", value=str(0).encode(), ephemeral=True, makepath=True)

            if elected_primary in self.replicas:
                self.replicas.remove(elected_primary)

            print(f"primary: {elected_primary} == address: {self.address}")
            if elected_primary == self.address:
                if self.zk.zk.exists("/brokers"):
                    print("self.zk.zk.exists({}) : {}".format("/brokers", self.zk.zk.get_children("/brokers")))

            print(f"zk primary replicas: {self.zk.zk.get_children(self.primary_replicas_path)}")
            print(f"self backup replicas: {self.replicas}")

    def update_primary_threshold(self, address, add=True):
        with self._lock:
            if address in self.primary_replicas:
                if add:
                    self.primary_replicas[address] += 1
                else:
                    self.primary_replicas[address] -= 1


