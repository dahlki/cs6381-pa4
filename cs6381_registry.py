import random

import zmq
import json
import cs6381_constants as constants
import time
import argparse
import threading
import logging

from cs6381_history import History
from cs6381_zkelection import Election
from cs6381_registryhelper import RegistryHelper
from cs6381_util import get_system_address
from cs6381_zkwatcher import Watcher
from cs6381_zkclient import ZooClient
from kad_client import KademliaClient

def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Registry")
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")
    parser.add_argument("-p", "--publishers", type=int, default=1,
                        help="number of publishers that need to register before dissemination begins")
    parser.add_argument("-s", "--subscribers", type=int, default=1,
                        help="number of subscribers that need to register before dissemination begins")
    parser.add_argument("-b", "--brokers", type=int, default=1,
                        help="number of brokers that need to register before dissemination begins")
    parser.add_argument("-r", "--registries", type=int, default=1,
                        help="number of registries; used for data collection info only")
    parser.add_argument("-c", "--create", default=False, action="store_true",
                        help="Create a new DHT ring, otherwise we join a DHT")
    parser.add_argument("-l", "--debug", default=logging.WARNING, action="store_true",
                        help="Logging level (see logging package): default WARNING else DEBUG")
    parser.add_argument("-i", "--ipaddr", type=str, default=None, help="IP address of any existing DHT node")
    parser.add_argument("-o", "--port", help="port number used by one or more Kademlia DHT nodes", type=int, default=8468)
    parser.add_argument("-t", "--topo", help="mininet topology; used for data collection info only",
                        choices=["linear", "tree"], type=str)

    return parser.parse_args()


class RegistryServer:

    def __init__(self, topo, strategy, pubs=1, subs=1, brokers=1, registries=1, create=False):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.cache_socket = self.context.socket(zmq.REP)
        self.socket_registry_data = self.context.socket(zmq.PUB)
        self.ip = get_system_address()
        self.kad_ipaddr = None
        self.kad_port = None
        self.strategy = strategy

        self.debug = False
        self.first_node = create
        self.create = create

        self.kad_client = None
        self.helper = None
        # self.nodes = [self.ip]

        self.lock = threading.Condition()

        self.brokers = brokers if strategy == constants.BROKER else 0
        self.pubs = pubs
        self.subs = subs
        self.registries = registries
        self.topo = topo

        self.brokers = []
        self.zk_brokers = []

        self.zoo_client = ZooClient(constants.REGISTRY, self.ip, constants.REGISTRY_PORT_NUMBER)
        self.zoo_client.join_election()
        self.zoo_client.register(constants.KAZOO_REGISTRIES_PATH, f"{self.ip}:{constants.REGISTRY_PORT_NUMBER}")
        self.zk = self.zoo_client.get_zk()
        self.get_watcher(constants.KAZOO_BROKERS_PATH, self.brokers_watcher_callback, True)
        self.kad_registries = []



    def get_watcher(self, path, callback, do_watch_children):
        watcher = Watcher(self.zk, constants.REGISTRY, path)
        watcher.watch(callback, do_watch_children)
        return watcher

    def brokers_watcher_callback(self, path, data):
        print(f"in broker cb in registry: {path} {data}")
        print(f"registry current broker data: {self.brokers}")
        self.zk_brokers = data
        # difference = list(set(current_brokers).difference(data))

    def check_brokers(self):
        while True:
            if not(set(self.zk_brokers) == set(self.brokers)):
                current_brokers = self.helper.get_broker_nodes()
                self.helper.set_broker_nodes(self.zk_brokers)
                self.brokers = self.zk_brokers
                print(f"brokers list!!!!!!!!!!!!!!!!!!!: {self.brokers}")

    def get_registry_ip(self, path, children):
        print(f"registry server - registries ChildrenWatch: {children}")
        # if children:
        #     children_without_self = [child for child in children if self.ip not in child]
        #     # children_without_self = children
        #     print(f"registries children without self: {children_without_self}")
        #     if children_without_self:
        #         server_ip = random.choice(children_without_self).split(":")[0]
        #         self.kad_ipaddr = server_ip

        print("registries children: {}".format(children))
        print("kad_ipaddr: {}".format(self.kad_ipaddr))

    def broker_registration(self, address, port):
        self.socket.send_string(json.dumps(self.brokers))

    def pub_registration(self, address, port, topics):
        connection = 'tcp://{}:{}'.format(address, port)
        topics_to_register = json.loads(topics)
        # load topics into registry
        for topic in topics_to_register:
            print("inserting pub topic into registry: {}, address: {}".format(topic, connection))
            self.helper.set_registry(topic, connection)

        if self.strategy == constants.BROKER:
                self.socket.send_string("successfully registered pub for {} at {}!".format(topics, connection))

        else:
            self.socket.send_string("successfully registered pub for {} at {}!".format(topics, connection))
            self.notify_new_pub_connection(topics_to_register, connection)

    def start_receiving(self):
        self.socket.bind('tcp://*:{}'.format(constants.REGISTRY_PORT_NUMBER))
        print("registry starting to receive requests")
        while True:
            message = self.socket.recv_string(0)
            message = message.split()
            print("message received at server", message)

            if self.socket.getsockopt(zmq.RCVMORE):
                topics = self.socket.recv_json(1)
            else:
                topics = []

            action, *info = message

            if message:
                if action == constants.REGISTER:
                    role, address, port, history = info
                    print("registry request received to {}: {} {} {} {}".format(action, role, address, port, topics, history))

                    if role == constants.BROKER:
                        print("broker registry request received")
                        self.broker_registration(address, port)

                    if role == constants.PUB:
                        # create pub connection string for topic registration
                        self.pub_registration(address, port, topics)

                    if role == constants.SUB:
                        print("register sub")

                elif action == constants.DISCOVER:
                    topic = info[0]
                    print('retrieving address for: {}'.format(topic))
                    if topic:
                        address = self.helper.get(topic)
                        print(address)
                        self.socket.send_string(json.dumps(address))

                elif action == constants.REGISTRY:
                    registry = self.helper.get_registry()
                    print("GOT!:", registry)
                    self.socket.send_string(json.dumps(registry))
                    print("registry sent")

                elif action == constants.REPLICA:
                    replica_number, replica_address, data = info
                    print(f"\n\ngot message for replica!!! {replica_number} {replica_address} {data}")
                    self.helper.set_value_to_list('primaries', f"{replica_number} {replica_address}")
                    self.socket.send_string("HI FROM REGISTRY!!!!!!!!!!")
                    value = self.helper.get("primaries")
                    print(f"{value}\n\n")
                elif action == constants.BROKER:
                    brokers = self.helper.get_broker_nodes()
                    broker = brokers.pop()
                    print(brokers)
                    print(broker)
                    print(brokers)
                    self.helper.set_broker_nodes(brokers)
                    self.socket.send_string(json.dumps(broker))

    def notify_new_pub_connection(self, topics, connection):
        self.lock.acquire()
        try:
            self.lock.notify()
            for topic in topics:
                topic_connection = "%s %s" % (topic, connection)
                print(f"NEW PUB TOPIC: {topic}")
                is_new_topic = self.helper.set_topic_index(topic_connection)
                # if is_new_topic:
                print("NOTIFYING SUBSCRIBERS OF NEW PUB FOR TOPIC: {} on registry node {}".format(topic_connection, self.ip))
                self.socket_registry_data.send_string(topic_connection)
        finally:
            self.lock.release()

    def start_cache_service(self):
        self.cache_socket.bind('tcp://*:{}'.format(constants.CACHE_PORT_NUMBER))
        while True:
            message = self.cache_socket.recv_string(0)
            message = message.split()
            action, role, address, topic, history = message

            if self.cache_socket.getsockopt(zmq.RCVMORE):
                message_history = self.cache_socket.recv_json(1)
            else:
                message_history = []

            if message:
                print("\nSSSSSSSSSSSSSSSSSSSSS\n")
                print(message)

                if action == constants.HISTORY:
                    if role == constants.SUB:
                        print(f"in registry getting HISTORY for SUB: {role} {topic} {history} {address}")
                        history_from_kad = self.helper.get_pub_history(f"{topic}-{history}-{address}")
                        print(history_from_kad)
                        if history_from_kad is not None:
                            self.cache_socket.send_string(history_from_kad)
                        else:
                            self.cache_socket.send_string("no history")

                    elif role == constants.PUB:
                        print(f"in registry setting HISTORY for PUB: {role} {message_history} {topic}")
                        self.helper.set_pub_history(f"{topic}-{history}-{address}", message_history)
                        history_from_kad = self.helper.get_pub_history(f"{topic}-{history}-{address}")
                        # for message in history_from_kad:
                        #     print(message)
                        self.cache_socket.send_string("HI FROM REGISTRY!!!!!!!!!!")


    def create_kad_client(self, create, ip, port):
        print(f"KAD: create: {create}, port: {port}, ip: {ip}")
        self.kad_client = KademliaClient(create, port, [(ip, port)], self.debug)
        self.helper = RegistryHelper(self.kad_client)

    def start(self, args):
        print("my ip - registry:", self.ip)
        print("starting registry server on port: {}".format(constants.REGISTRY_PORT_NUMBER))
        print("args", args)
        self.first_node = True if self.create else False
        self.debug = args.debug
        self.kad_port = args.port
        self.kad_ipaddr = args.ipaddr if (args.ipaddr is not None and not args.create) else self.ip


        registry_thread = threading.Thread(target=self.start_receiving)
        registry_thread.setDaemon(True)

        cache_thread = threading.Thread(target=self.start_cache_service)
        cache_thread.setDaemon(True)

        print(f"registry {self.ip} connecting to kad registry {self.kad_ipaddr}")
        self.create_kad_client(self.create, self.kad_ipaddr, self.kad_port)

        registry_thread.start()
        cache_thread.start()

        self.check_brokers()


def main():
    args = parseCmdLineArgs()
    registry = RegistryServer(args.topo, args.disseminate, args.publishers, args.subscribers, args.brokers, args.registries, args.create)
    registry.start(args)


if __name__ == "__main__":
    main()
