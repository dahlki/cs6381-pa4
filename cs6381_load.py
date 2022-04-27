from threading import Lock

from kazoo.client import KazooClient

import cs6381_constants
from cs6381_constants import KAZOO_IP, KAZOO_PORT
from cs6381_zkwatcher import Watcher


class LoadBalancer:
    def __init__(self, role, ip, port, cb, replica_count=1, min=1, max=3):

        self.zk = KazooClient(hosts='{}:{}'.format(KAZOO_IP, KAZOO_PORT))
        self.zk.start()

        self.role_replicas_path = f"/{role}-replicas"
        self.role_replicas_data_path = self.role_replicas_path + "/{}"

        self.election = None
        self.election_watcher = None
        self.watcher = None

        self.backup_election = None
        self.backup_election_watcher = None

        self.role = role

        self.ip = ip
        self.port = port
        self.address = f"{ip}:{port}"

        self.replica_count = replica_count
        self.current_count = 1

        self.replica_count_list = []
        self.primary_replicas = {}

        self.min = min
        self.max = max
        self.connection_count = 0

        self.lock = Lock()

        self.callback = cb
        self.do_register = True
        self.setup_replica_count_list()



    def setup_replica_count_list(self):
        print("SILVIA SILVIA SILVIA SILVIA SILVIA SILVIA")
        print(self.replica_count)
        self.replica_count_list = list(range(1, self.replica_count + 1))
        print(f"replica_count_list: {self.replica_count_list}")

        if not self.zk.exists("/replica"):
            self.zk.create("/replica", value=str(self.current_count).encode(), ephemeral=True, makepath=True)
        else:
            current_count_value = self.zk.get("/replica", self.get_callback)
            print(f"USE THIS COUNT? : {current_count_value}")
            self.current_count = int(current_count_value[0].decode())
            self.current_count = self.current_count + 1 if self.current_count + 1 <= len(self.replica_count_list) else 1
            with self.lock:
                self.zk.set("/replica", value=str(self.current_count).encode())

        # print(f"replica current count: {self.current_count}")
        # print(f"current replica count from ZK: ", self.zk.get("/replica"))

        watcher = Watcher(self.zk, cs6381_constants.BROKER, "/replica", str(self.current_count))
        watcher.watch(self.replica_watcher_callback, False)

        # self.create_replica_nodes()

    def get_callback(self, data):
        print("HIHIHIHIHIHIHIHIHIHIHIHI")
        print(f"in get_callback - data: {data.type} - path: {data.path} - stat: {data.state}")

    def replica_watcher_callback(self, path, data):
        print(f"in replica_watcher_callback_watcher_callback - path: {path} - data: {data}")
        if data:
            count = data.decode()
            print(f"replica_watcher_callback count: {count}")
            self.add_replica_as_contender(count)
            self.current_count = int(count) + 1 if int(count) + 1 <= len(self.replica_count_list) else 1
        print(f"replica current count: {self.current_count}")

    def add_replica_as_contender(self, count):
        if self.do_register:
            print(f"\n*********adding {self.role} - {self.address} as contender for replication number {count}\n")
            self.add_replica(count)
            self.do_register = False

    def add_replica(self, count):
        count = str(count)
        path = f"/{count}-election"
        if self.zk.exists(path):
            self.zk.get(path, self.cb)
        else:
            print("create node for: {}".format(path))
            self.zk.create(path, sequence=False, makepath=True)
        zk_election = self.zk.Election(path, self.address)
        zk_election.run(self.replica_election_callback, count)

    def cb(self, data):
        print("SONN SONN SONN SONN SONN SONN")
        print(f"in get cb: {data}")

    def replica_election_callback(self, count):
        count = str(count)
        path = f"/{count}"
        print(f"\n{count} leader elected!")
        children = self.zk.get_children("/")
        print(f"\nchildren nodes: {children}")

        watcher = Watcher(self.zk, self.role, path, f"{self.ip}:{self.port}")
        watcher.watch(self.callback)

    # def default_callback(self, path, data):
    #     print(f"SILVIA SILVIA SILVIA in REPLICA election default callback, path: {path} data: {data}")
    #     if path:
    #         replica_num = path[1:]
    #         if data:
    #             broker_address = data.decode()
    #             if not self.zk.exists("/primaries"):
    #                 self.zk.create("/primaries")
    #
    #             primaries = self.zk.get("primaries")
    #             primaries_children = self.zk.get_children("primaries")
    #             print(primaries)
    #             print(primaries_children)
    #
    #             self.zk.Election(f"/{replica_num}-election", broker_address).cancel()



