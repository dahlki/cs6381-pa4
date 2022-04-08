import threading

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from cs6381_constants import KAZOO_REGISTRIES_PATH


class Watcher:
    def __init__(self, zk, role, path, ip=None, port=None):
        self.role = role
        self.path = path
        self.ip = ip
        self.port = port
        self.address = "{}:{}".format(ip, port)

        self.zk = zk
        self.data = {}
        self.cb = self.default_callback

    def watch(self, callback=None):
        if callback is not None:
            self.cb = callback

        @self.zk.DataWatch(self.path)
        def watcher(data, stat):
            print(f"\nwatcher triggered for {self.path} by {self.role}. data={data}, stat={stat}")
            print("\n*********** Inside watch_znode_data_change *********")
            print(("Data changed for znode {}: data = {}, stat = {}".format(self.path, data, stat)))
            print("*********** Leaving watch_znode_data_change *********\n")
            if data is None:
                print("in watcher - data is none")
                try:
                    if self.zk.exists(self.path):
                        value, stat = self.zk.get(self.path)
                        print("Watcher got details of {}: value = {}, stat = {}".format(self.path, value, stat))
                    elif self.ip is not None:
                        print("Watcher creating path: {} with data: {}".format(self.path, self.address))
                        self.zk.create(self.path, value=self.address.encode(), ephemeral=True, makepath=True)

                except NoNodeError:
                    print("no node to watch for Watcher")
                    pass

            self.data[self.path] = data
            return self.cb(self.path, data)

    def get_data(self, path):
        return self.data[path]

    def default_callback(self, path, data):
        print(f"in default callback, path: {path} data: {data}")
