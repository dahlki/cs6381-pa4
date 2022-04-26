import json

from kazoo.client import KazooClient
from kazoo.client import KazooState

import cs6381_constants as constants
from cs6381_constants import KAZOO_IP, KAZOO_PORT
from cs6381_zkelection import Election
from cs6381_zkwatcher import Watcher


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
        return self.zk_election

    def get_watcher(self, path, callback, watch_children=False):
        try:
            watcher = Watcher(self.zk, self.role, path)
            watcher.watch(callback, watch_children)
            return watcher
        except Exception:
            print("exception occurred in zkclient watcher")

    def register(self, path, address):
        # self.zk.delete(f"/{path}")
        print(f"registering zk client: {self.role} for path: {path} with address: {address}")
        print(not self.zk.exists(path))
        if not self.zk.exists(path):
            print(f"creating node path: {path}")
            self.zk.create(path)

        print(f"CLIENT for: {path}")
        node_path = f"{path}/{address}"
        if self.zk.exists(node_path):
            print("{} znode indeed exists; get value".format(path))
            value = self.zk.get_children(path)
            print(("Details of znode {} children: value = {}".format(path, value)))
            value = self.zk.get(node_path)
            print(("Details of znode {}: value = {}".format(path, value)))

        else:
            print("create node for: {}".format(node_path))
            self.zk.create(node_path, value=address.encode(), makepath=True, ephemeral=True)

    def register_topic(self, topic):
        topic_election = Election(self.zk, f"publisher/{topic}", self.ip, self.port)
        topic_election.register()
