import json

from kazoo.client import KazooClient
from kazoo.client import KazooState

import cs6381_constants as constants
from cs6381_constants import KAZOO_IP, KAZOO_PORT
from cs6381_zkelection import Election


class ZooClient:
    def __init__(self, role, ip, port):

        self.zk = KazooClient(hosts='{}:{}'.format(KAZOO_IP, KAZOO_PORT))
        self.zk.add_listener(self.listener_state)
        print("STATE after connect = {}".format(self.zk.state))
        self.zk.start()

        self.role = role
        self.ip = ip
        self.port = port
        self.address = f"{self.ip}:{self.port}"

        self.path = "/{}".format(role)
        self.elected_path = "/{}-election".format(role)

        self.topics = []

        self.zk_election = None
        self.election = None

        # self.zk.delete(path="/registries0000000235", recursive=True)
        # self.zk.delete(path="/pub", recursive=True)
        # self.zk.delete(path="/pub-election", recursive=True)
        # self.zk.delete(path="/sub", recursive=True)
        # self.zk.delete(path="/sub-election", recursive=True)
        # self.zk.delete(path="/broker", recursive=True)
        # self.zk.delete(path="/registry", recursive=True)
        # self.zk.delete(path="/registry-election", recursive=True)
        # self.zk.delete(path="/broker-election", recursive=True)

    @staticmethod
    def listener_state(state):
        if state == KazooState.LOST:
            print("Current state is now = LOST")
        elif state == KazooState.SUSPENDED:
            print("Current state is now = SUSPENDED")
        elif state == KazooState.CONNECTED:
            print("Current state is now = CONNECTED")
        else:
            print("Current state now = UNKNOWN !! Cannot happen")

    def get_zk(self):
        return self.zk

    def join_election(self):
        self.election = Election(self.zk, self.role, self.ip, self.port)
        self.election.register()
        self.zk_election = self.election.zk_election

    # def register_pub_topics(self, topics=None):
    #     if topics:
    #         self.topics = topics
    #     print("REGISTERING PUB TOPICS")
    #     if self.role == constants.PUB:
    #         path = "/pub"
    #         if self.zk.exists(path):
    #             print("{} znode indeed exists; get value".format(path))
    #             value, stat = self.zk.get(path)
    #             print(("Details of znode {}: value = {}, stat = {}".format(path, value, stat)))
    #
    #         else:
    #             print("create node for: {}".format(path))
    #             self.zk.create(path, sequence=True)
    #
    #         if self.topics:
    #             print(f"registering pub topics: {self.topics}")
    #             for topic in self.topics:
    #                 topic_path = f"/pub/{topic}"
    #                 if self.zk.exists(topic_path):
    #                     value, stat = self.zk.get(topic_path)
    #                     value = json.loads(value).append(self.address)
    #                     self.zk.set(topic_path, json.dumps(value).encode())
    #                 else:
    #                     print("*******" + topic_path)
    #                     address = [self.address]
    #                     self.zk.create(topic_path, value=json.dumps(address).encode(), makepath=True, ephemeral=True)
    #                 value, stat = self.zk.get(topic_path)
    #                 print(("New value at znode {}: value = {}, stat = {}".format(topic_path, value, stat)))
    #
    # def register_registry(self, address):
    #     print("REGISTERING REGISTRY")
    #     path = "/registries"
    #     if self.zk.exists(path):
    #         print("{} znode indeed exists; get value".format(path))
    #         value, stat = self.zk.get(path)
    #         value = value.decode()
    #         if len(value):
    #             value += f",{address}"
    #             self.zk.set(path, value.encode())
    #         print(("Details of znode {}: value = {}, stat = {}".format(path, value, stat)))
    #
    #     else:
    #         print("create node for: {}".format(path))
    #         self.zk.create(path, value=address.encode(), makepath=True, ephemeral=True)


        # value, stat = self.zk.get(registry_path)
        # print(("New value at znode {}: value = {}, stat = {}".format(registry_path, value, stat)))



