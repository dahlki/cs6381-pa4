from kazoo.client import KazooClient
from kazoo.client import KazooState

import cs6381_constants as constants
from cs6381_constants import KAZOO_IP, KAZOO_PORT
from cs6381_zkwatcher import Watcher


class Election:
    def __init__(self, zk, role, ip, port, **kwargs):
        self.__dict__.update(kwargs)
        # print("**election kwargs: {}".format(self.__dict__))
        self.role = role
        self.ip = ip
        self.port = port
        self.topics = self.__dict__.get("topics") if "topics" in kwargs else []

        self.path = "/{}".format(role)
        self.elected_path = "/{}-election".format(role)
        self.primary = "/primary-{}".format(role)

        self.address = '{}:{}'.format(ip, port)

        self.zk_election = None

        self.zk = zk

    def register(self):

        print(self.role, self.address, self.port, self.path, self.elected_path)
        print('\nElection registering: {} with value: {}\n'.format(self.elected_path, self.address))

        if self.zk.exists(self.elected_path):
            print("{} znode indeed exists; get value".format(self.elected_path))
            value, stat = self.zk.get(self.elected_path)
            # print(("Details of znode {}: value = {}, stat = {}".format(self.elected_path, value, stat)))

        else:
            print("create node for: {}".format(self.elected_path))
            self.zk.create(self.elected_path, sequence=False, makepath=True)

        self.zk_election = self.zk.Election(self.elected_path, self.address)
        self.zk_election.run(self.elected)
        print("STATE after election register = {}".format(self.zk.state))

    def elected(self):
        print(f"\n{self.role} leader elected!")
        children = self.zk.get_children("/")
        print(f"\nchildren nodes: {children}")
        print(f"contenders: {self.zk_election.contenders()}")
        # if self.zk.exists("/registries"):
        #     children_children = self.zk.get_children("/registries")
        #     print(f"\nchildren nodes: {children_children}")
        watcher = Watcher(self.zk, self.role, self.path, f"{self.ip}:{self.port}")
        watcher.watch(self.default_callback)

    def default_callback(self, path, data):
        print(f"in election default callback, path: {path} data: {data}")

    # def load_balancer_election_callback(self, path, data):
    #     print(f"ELECTION WATCH callback - load_balancer_election_callback, role: {self.role}, path: {path} data: {data}")
    #     if data is not None:
    #         self.primary = data.decode()
    #     print(f"PRIMARY: {self.primary}")
    #     # if self.zk.exists(self.elected_path):
    #     #     print(f"elected_path: {self.zk.get(self.elected_path)}")
    #     if self.zk.exists("/brokers"):
    #         path = "/brokers"
    #         brokers = self.zk.get_children(path)
    #         print(f"brokers path: {brokers}")
    #         if brokers:
    #             backup_brokers = [broker for broker in brokers if self.primary not in broker]
    #             print(f"backup brokers: {backup_brokers}")
    #
    #     print("END ELECTION WATCH callback - load_balancer_election_callback\n\n")

