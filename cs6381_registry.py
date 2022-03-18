import zmq
import json
import cs6381_constants as constants
import time
import argparse
import threading
import logging

from cs6381_registryhelper import RegistryHelper
from cs6381_util import get_system_address
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
    parser.add_argument("-r", "--registries", type=int, default=1,
                        help="number of registries; used for data collection info only")
    parser.add_argument("-c", "--create", default=False, action="store_true",
                        help="Create a new DHT ring, otherwise we join a DHT")
    parser.add_argument("-l", "--debug", default=logging.WARNING, action="store_true",
                        help="Logging level (see logging package): default WARNING else DEBUG")
    parser.add_argument("-i", "--ipaddr", type=str, default=None, help="IP address of any existing DHT node")
    parser.add_argument("-o", "--port", help="port number used by one or more DHT nodes", type=int, default=8468)
    parser.add_argument("-t", "--topo", help="mininet topology; used for data collection info only",
                        choices=["linear", "tree"], type=str)

    return parser.parse_args()


class RegistryServer:

    def __init__(self, topo, strategy, pubs=1, subs=1, registries=1):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket_start_notification = self.context.socket(zmq.PUB)
        self.socket_registry_data = self.context.socket(zmq.PUB)
        self.ip = get_system_address()
        self.ipaddr = None
        self.strategy = strategy
        self.broker = 1 if strategy == constants.BROKER else 0
        self.pubs = pubs
        self.subs = subs
        self.registries = registries
        self.wait = True
        self.first_node = False

        self.kad_client = None
        self.helper = None
        self.nodes = [self.ip]

        self.lock = threading.Condition()
        self.topo = topo

    def check_start(self):
        pubs = self.helper.get(constants.PUB_COUNT)
        subs = self.helper.get(constants.SUB_COUNT)
        broker = self.helper.get(constants.BROKER_COUNT)

        if (broker is not None and broker <= 0) and (pubs is not None and pubs <= 0) and (subs is not None and subs <= 0):
            self.helper.set("start", True)
            return True
        return False

    def should_start(self):
        self.socket_start_notification.bind('tcp://*:{}'.format(constants.REGISTRY_PUSH_PORT_NUMBER))
        while not(self.check_start()):
            pass
        print("SENDING START SIGNAL!!!!!!!!!!")
        self.socket_start_notification.send_string("start")

    def broker_registration(self, address, port):
        self.helper.set_broker_ip(address)
        self.helper.set_broker_port(port)
        self.socket.send_string("successfully registered broker's address {}!".format(address))

    def pub_registration(self, address, port, topics):
        connection = 'tcp://{}:{}'.format(address, port)
        topics_to_register = json.loads(topics)
        # load topics into registry
        for topic in topics_to_register:
            print("inserting pub topic into registry: {}, address: {}".format(topic, connection))
            self.helper.set_registry(topic, connection)

        if self.strategy == constants.BROKER:
            broker_ip = self.helper.get_broker_ip()
            if broker_ip:
                self.socket.send_string("successfully registered pub for {} at {}!".format(topics, connection),
                                        0 | zmq.SNDMORE)
                self.socket.send_string(broker_ip, 1 | 0)
            else:
                self.socket.send_string("successfully registered pub for {} at {}!".format(topics, connection))

        else:
            self.socket.send_string("successfully registered pub for {} at {}!".format(topics, connection))
            self.notify_new_pub_connection(topics_to_register, connection)

    def start_receiving(self):
        self.socket.bind('tcp://*:{}'.format(constants.REGISTRY_PORT_NUMBER))

        print("start_receiving")

        self.helper.set_registry("nodes", self.ip)
        self.helper.set_registry_node(self.ip)
        if self.first_node:
            self.helper.set(constants.PUB_COUNT, self.pubs)
            self.helper.set(constants.SUB_COUNT, self.subs)
            self.helper.set(constants.BROKER_COUNT, self.broker)
            self.helper.set("fileData", "{} {} {} {}".format(self.topo, self.pubs, self.subs, self.registries))

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
                    role, address, port = info
                    print("registry request received to {}: {} {} {} {}".format(action, role, address, port, topics))

                    if role == constants.BROKER:
                        print("broker registry request received")
                        self.broker_registration(address, port)

                        broker_num = self.helper.get(constants.BROKER_COUNT)
                        if broker_num > 0:
                            print("******* updating broker num")
                            self.helper.set(constants.BROKER_COUNT, broker_num - 1)

                    if role == constants.PUB:
                        # create pub connection string for topic registration
                        self.pub_registration(address, port, topics)

                        pub_nums = self.helper.get(constants.PUB_COUNT)
                        print("current pub count: {}".format(pub_nums))
                        if pub_nums > 0:
                            pub_nums -= 1
                            print("******* updating pub num to {}".format(pub_nums))
                            self.helper.set(constants.PUB_COUNT, pub_nums)
                        self.socket_registry_data.send_string(constants.PUB_COUNT, pub_nums)

                    if role == constants.SUB:
                        sub_nums = self.helper.get(constants.SUB_COUNT)
                        print("current pub count: {}".format(sub_nums))
                        if sub_nums > 0:
                            sub_nums -= 1
                            print("******* updating sub num to {}".format(sub_nums))
                            self.helper.set(constants.SUB_COUNT, sub_nums)
                        meta_data = self.helper.get("fileData")
                        self.socket.send_string("success {}".format(meta_data))

                elif action == constants.DISCOVER:
                    topic = info[0]
                    print('retrieving address for: {}'.format(topic))
                    if topic:
                        address = self.helper.get(topic)
                        print(address)
                        self.socket.send_string(json.dumps(address))

                elif action == constants.REGISTRY:
                    registry = self.helper.get_registry()
                    # registry = self.kad_client.get("registry")
                    print("GOT!:", registry)
                    if registry:
                        self.socket.send_string(json.dumps(registry))
                        print("registry sent")
                elif action == constants.REGISTRY_NODES:
                    nodes = self.helper.get_registry_nodes()
                    self.socket.send_string(json.dumps(nodes))

    def notify_new_pub_connection(self, topics, connection):
        self.lock.acquire()
        try:
            self.lock.notify()
            for topic in topics:
                topic_connection = "%s %s" % (topic, connection)
                is_new_topic = self.helper.set_topic_index(topic_connection)
                if is_new_topic:
                    print("NOTIFYING SUBSCRIBERS OF NEW PUB FOR TOPIC: {} on registry node {}".format(topic_connection, self.ip))
                    self.socket_registry_data.send_string(topic_connection)
        finally:
            self.lock.release()

    def start_registry_data(self):
        self.socket_registry_data.bind('tcp://*:{}'.format(constants.REGISTRY_PUB_PORT_NUMBER))
        while True:
            nodes = self.helper.get_registry_nodes()
            if nodes is not None:
                for node in nodes:
                    if not(node in self.nodes):
                        self.socket_registry_data.send_string("{} {}".format(constants.REGISTRY_NODES, nodes))
                        self.nodes.append(node)
            time.sleep(2)

    def start(self, args):
        print("my ip - registry:", self.ip)
        print("starting registry server on port: {}".format(constants.REGISTRY_PORT_NUMBER))
        print("args", args)
        self.first_node = True if args.create else False

        # self.socket.bind('tcp://*:{}'.format(constants.REGISTRY_PORT_NUMBER))

        registry_thread = threading.Thread(target=self.start_receiving)
        registry_thread.setDaemon(True)

        registry_pub_thread = threading.Thread(target=self.start_registry_data)
        registry_pub_thread.setDaemon(True)

        self.ipaddr = args.ipaddr if (args.ipaddr is not None and not args.create) else self.ip
        self.kad_client = KademliaClient(args.create, args.port, [(self.ipaddr, args.port)])
        self.helper = RegistryHelper(self.kad_client)

        registry_thread.start()
        registry_pub_thread.start()

        should_start_thread = threading.Thread(target=self.should_start)
        should_start_thread.setDaemon(True)
        should_start_thread.start()


def main():
    args = parseCmdLineArgs()
    registry = RegistryServer(args.topo, args.disseminate, args.publishers, args.subscribers, args.registries)
    registry.start(args)


if __name__ == "__main__":
    main()
