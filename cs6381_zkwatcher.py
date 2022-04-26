import threading

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError

from cs6381_constants import KAZOO_REGISTRIES_PATH


class Watcher:
    def __init__(self, zk, role, path, address=None):
        self.role = role
        self.path = path
        # 10.0.0.2:1005
        self.address = address

        self.zk = zk
        self.data = {}
        self.cb = self.default_callback

    def watch(self, callback=None, watch_children=False):
        print(f"STARTING TO WATCH {self.path} with children: {watch_children} by {self.role}")
        if callback is not None:
            self.cb = callback
        if not watch_children:
            @self.zk.DataWatch(self.path)
            def watcher(data, stat):
                print("\n\n********************************************************")
                print(f"watcher triggered for {self.path} by {self.role}. data={data}")
                print("\n*********** Inside watch_znode_data_change *********")
                print(("Data changed for znode {}: data = {}, stat = ".format(self.path, data, stat)))
                print("*********** Leaving watch_znode_data_change *********\n")
                print("********************************************************\n")

                if data is None:
                    print("in watcher - data is none")
                    try:
                        if self.zk.exists(self.path):
                            value, stat = self.zk.get(self.path)
                            print("Watcher got details of {}: value = {}, stat = ".format(self.path, value, stat))
                        elif self.address is not None:
                            print("Watcher creating path: {} with data: {}".format(self.path, self.address))
                            self.zk.create(self.path, value=self.address.encode(), ephemeral=True, makepath=True)

                    except NoNodeError:
                        print("no node to watch for Watcher")
                        pass
                    except NodeExistsError:
                        print("node exists to watch for Watcher")
                        pass

                self.data[self.path] = data

                return self.cb(self.path, data)
        else:
            @self.zk.ChildrenWatch(self.path)
            def watcher(children):
                print("\n\n********************************************************")
                print(f"children watcher triggered for {self.path} by {self.role}. children={children}")
                print("\n*********** Inside watch_znode_data_children_change *********")
                print(("Data changed for znode {}: children = {}".format(self.path, children)))
                print("*********** Leaving watch_znode_data_children_change *********\n")
                print("********************************************************\n\n")

                return self.cb(self.path, children)

    def get_data(self, path):
        return self.data[path]

    def default_callback(self, path, data):
        print(f"in default watcher callback, path: {path} data: {data}")

