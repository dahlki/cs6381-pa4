import random
import threading

import zmq
import json
import cs6381_constants as constants
import time

from cs6381_metadata import MetaData
from cs6381_load import LoadBalancer
from cs6381_zkwatcher import Watcher
from cs6381_zkclient import ZooClient


class Registry:

    def __init__(self, role, address="localhost", port=None, strategy=None, client=None, registry_ip="10.0.0.1", replicas=1):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket_registry_data = self.context.socket(zmq.SUB)
        self.poller = zmq.Poller()

        self.role = role
        self.topics = None
        self.history = None
        self.address = address
        self.port = port
        self.strategy = strategy
        self.registry_server_ip = "localhost" if self.address == "localhost" else None
        self.client = client
        self.should_start = True

        self.num_pubs = None
        self.num_subs = None
        self.num_brokers = None
        self.num_registries = None
        self.topo = None

        self.registry_ips = []
        self.topic_thread = None
        self.lock = threading.Lock()

        self.zoo_client = ZooClient(role, self.address, self.port)
        self.zk = self.zoo_client.get_zk()
        self.join_election()

        self.zoo_client.get_watcher(constants.KAZOO_REGISTRIES_PATH, self.get_registry_ip, True)

        self.topic_registry = {}
        self.replica_count = replicas
        self.replica_count_list = []
        # self.setup_replica_count_list()
        self.join_election()

    def setup_replica_count_list(self):
        print(self.replica_count)
        self.replica_count_list = list(range(1, self.replica_count + 1))
        print(f"replica_count_list: {self.replica_count_list}")

        if self.strategy == constants.BROKER and self.role != constants.BROKER:
            for num in self.replica_count_list:
                print("Must start Broker App first!")
                self.zoo_client.get_watcher(f"/{num}", self.sub_callback)

    def join_election(self):
        if self.role == constants.BROKER:
            self.zoo_client.join_election()
            self.zoo_client.register(constants.KAZOO_BROKER_PATH, f"{self.address}:{self.port}")
        self.zoo_client.get_watcher(constants.KAZOO_BROKER_PATH, self.broker_replica_watcher, True)


    def get_registry_ip(self, path, children):
        if children:
            print("registries children: {}".format(children))
            if self.registry_server_ip and any(self.registry_server_ip in child for child in children):
                print(f"registry client {self.role}, already connected to valid server: {self.registry_server_ip}")
                pass
            else:
                server_ip = random.choice(children).split(":")[0]
                self.registry_server_ip = server_ip
                self.connect_server()
                # if self.topics:
                #     self.register(self.topics)


    def intiiate_load_balancer(self):
        if self.role == constants.BROKER:
            print(f"sending replica count {self.replica_count} to load balancer")
            LoadBalancer(self.role, self.address, self.port, self.load_callback, self.replica_count)

    def load_callback(self, path, data):
        print(f"SILVIA SILVIA SILVIA in REPLICA election default callback, path: {path} data: {data}")
        if path:
            replica_num = path[1:]
            if data:
                broker_address = data.decode()
                if not self.zk.exists("/primaries"):
                    self.zk.create("/primaries")

                primaries = self.zk.get("primaries")
                primaries_children = self.zk.get_children("primaries")
                print(primaries)
                print(primaries_children)

                self.send_registry_replica(replica_num, broker_address, )

    def send_registry_replica(self, replica_num, address, ):
        self.socket.send_string('{} {} {} {}'.format(constants.REPLICA, replica_num, address, "hi"))

        try:
            message = self.socket.recv_string()
            if message:
                print(message)
                # self.zk.Election(f"/{replica_num}-election", address).cancel()

        except Exception:
            print("Exception occurred while registring replica!")
            raise

    def get_new_registry_data(self, topics):
        print("\nregistering SUB to new registry!!!!\n")

        self.socket_registry_data.connect('tcp://{}:{}'.format(self.registry_server_ip, constants.REGISTRY_PUB_PORT_NUMBER))
        self.registry_ips.append(self.registry_server_ip)

        for topic in topics:
            print("subscribing to: {}".format(topic))
            self.socket_registry_data.setsockopt_string(zmq.SUBSCRIBE, topic)

        self.poller.register(self.socket_registry_data, zmq.POLLIN)
        while True:
            events = dict(self.poller.poll(1000))
            if self.socket_registry_data in events and events[self.socket_registry_data] == zmq.POLLIN:
                print("RECEIVED MESSAGE!!!")
                message = self.socket_registry_data.recv_string()
                print("meta data message: ", message)
                topic, connection = message.split(" ", 1)
                print("registry client received META data for topic:", topic, connection)
                if self.strategy == constants.DIRECT and connection.startswith("tcp"):
                    self.client.connect(connection)
                    self.client.subscribe(topic)

    def connect_server(self):
        #  connect to Registry Server
        print("connecting to registry server at {} {}\n".format(self.registry_server_ip, constants.REGISTRY_PORT_NUMBER))
        self.socket.connect('tcp://{}:{}'.format(self.registry_server_ip, constants.REGISTRY_PORT_NUMBER))

    def disconnect_server(self):
        #  connect to Registry Server
        print("no registry available!\ndisconnecting to registry server at {} {}\n".format(self.registry_server_ip, constants.REGISTRY_PORT_NUMBER))
        self.socket.disconnect('tcp://{}:{}'.format(self.registry_server_ip, constants.REGISTRY_PORT_NUMBER))

    def register(self, topics=None, history=0):
        self.history = history
        self.topics = topics
        print("in register method for topics: {} ".format(topics))
        if topics is None:
            topics = []
        print("registering {} for {} dissemination strategy".format(self.role, self.strategy))

        if self.client is not None:
            # register broker
            if self.role == constants.BROKER:
                self.register_broker()
            # register publisher
            elif self.role == constants.PUB and len(topics) > 0:
                self.register_publisher(topics)
            # register subscriber
            elif self.role == constants.SUB:
                self.register_subscriber(topics)

    def register_broker(self):
        print('register {} {} {} {}'.format(self.role, self.address, self.port, None))
        self.socket.send_string('{} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port))

        try:
            message = self.socket.recv_string()
            if message:
                print("registry response received for register service: %s" % message)
                print("registered broker: {}\n".format(self.client))

                # self.intiiate_load_balancer()

                self.zoo_client.get_watcher("/publisher", self.pub_topic_watcher_callback, True)
                self.zoo_client.get_watcher("/subscriber", self.sub_topic_watcher_callback, True)
                self.client.start()

        except Exception:
            print("Exception occurred while registring broker!")
            raise

    def broker_replica_watcher(self, path, data):
        print("\n&&&&&&&&&&&&&&&& broker_replica_watcher...path: {} - data: {}\n".format(path, data))

    def pub_topic_watcher_callback(self, path, data):
        print("\n&&&&&&&&&&&&&&&& pub_topic_watcher_callback...path: {} - data: {}\n".format(path, data))
        if data:
            for topic in data:
                if "election" not in topic:
                    self.zoo_client.get_watcher(f"/publisher/{topic}", self.pub_topic_watcher_address_callback)

    def pub_topic_watcher_address_callback(self, path, data):
        print("\n&&&&&&&&&&&&&&&& pub_topic_watcher_callback...path: {} - data: {}\n".format(path, data))

        topic = path.replace("/publisher/", "")

        if "pub" not in self.topic_registry:
            self.topic_registry["pub"] = {}

        if data:
            address = data.decode()
            self.topic_registry["pub"][topic] = address
        else:
            self.topic_registry["pub"][topic] = {}

        print(f"topic registry: {self.topic_registry}")

    def sub_topic_watcher_callback(self, path, data):
        print("\n&&&&&&&&&&&&&&&& sub_topic_watcher_callback...path: {} - data: {}\n".format(path, data))
        if data:
            for topic in data:
                if "election" not in topic:
                    self.zoo_client.get_watcher(f"/subscriber/{topic}", self.sub_topic_watcher_address_callback)

    def sub_topic_watcher_address_callback(self, path, data):
        print("\n&&&&&&&&&&&&&&&& sub_topic_watcher_address_callback...path: {} - data: {}\n".format(path, data))

        topic = path.replace("/subscriber/", "")

        if "sub" not in self.topic_registry:
            self.topic_registry["pub"] = {}

        if data:
            address = data.decode()
            self.topic_registry["sub"][topic] = address
        else:
            self.topic_registry["sub"][topic] = {}

        print(f"topic registry: {self.topic_registry}")

    def pub_callback(self, path, data):
        print("in pub callback...path: {} - data: {}".format(path, data))
        if data is not None:
            data = data.decode()
            if ":" in data:
                ip, port = data.split(":")
                if constants.KAZOO_BROKER_PATH in path:
                    self.client.stop()
                    self.client.start(f"{ip}:{self.port}")
        print(f"{path}-{data}\n")

    def register_publisher(self, topics):
        # send registration info to registry server
        print('register {} {} {} {}'.format(self.role, self.address, self.port, topics))
        self.socket.send_string('{} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port),
                                0 | zmq.SNDMORE)
        self.socket.send_json(json.dumps(topics), 1 | 0)

        # response from registry server
        message = self.socket.recv_string(0)
        if message:
            print("registry response received for register service: %s" % message)
            self.zoo_client.join_election()
            for topic in topics:
                self.zoo_client.register_topic(topic)
                self.zoo_client.get_watcher(f"/{topic}", None)

            if self.strategy == constants.BROKER:
                self.zoo_client.get_watcher(constants.KAZOO_BROKER_PATH, self.pub_callback)
            else:
                self.client.start()
        else:
            print("error - publisher not registered!")

    def sub_callback(self, path, data):
        print("in sub callback...")
        print(path, data)
        if data is not None:
            data = data.decode()
            ip, port = data.split(":")

            if constants.KAZOO_BROKER_PATH in path:
                self.client.connect('tcp://{}:{}'.format(ip, self.port))
        print(f"{path}-{data}\n")

    def register_subscriber(self, topics):
        registry = None
        self.topic_thread = threading.Thread(target=self.get_new_registry_data, args=(topics,))
        self.topic_thread.setDaemon(True)
        self.topic_thread.start()

        self.socket.send_string('{} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port))
        message = self.socket.recv_string(0)
        print(f"register_subscriber message received: {message}")
        # success, *meta_data = message.split(" ")
        # print(meta_data[0])
        # if meta_data[0] != 'None':
        #     topo, pubs, subs, brokers, registries = meta_data
        # # used only for data file name creation
        #     if success == "success":
        #         self.topo = topo
        #         self.num_pubs = pubs
        #         self.num_subs = subs
        #         self.num_brokers = brokers
        #         self.num_registries = registries

        if message:
            print("register_subscriber!!!!")
            print(message)

            self.zoo_client.join_election()
            for topic in topics:
                self.zoo_client.register_topic(topic)
                self.zoo_client.get_watcher(f"/{topic}", None)

            while not registry:
                registry = self.get_registry(constants.SUB)
                time.sleep(1)

        print("REGISTRY:")
        print(registry)

        if self.strategy == constants.BROKER:
            print("Must start Broker App first!")
            self.zoo_client.get_watcher(constants.KAZOO_BROKER_PATH, self.sub_callback)

        for topic in topics:
            if self.strategy == constants.DIRECT:
                if topic in registry:
                    for connection in registry[topic]:
                        print(topic, registry[topic])
                        print("in registry - subscriber attempting to connect to %s for topic %s" % (connection, topic))
                        self.client.connect(connection)
                        self.client.subscribe(topic)
            elif self.strategy == constants.BROKER:
                self.client.subscribe(topic)

        self.client.start(self.num_pubs, self.num_subs, self.num_brokers, self.num_registries, self.strategy, self.topo)

    def get_topic_connection(self, topic):
        self.socket.send_string('{} {}'.format(constants.DISCOVER, topic))
        message = self.socket.recv()
        message_string = message.decode("utf-8")
        if message and message_string != "null":
            print("registry response received for get_connection service for %s topic: %s" % (topic, message_string))
            return json.loads(message)

    def get_registry(self, client):
        self.socket.send_string('{} {}'.format(constants.REGISTRY, client))
        message = self.socket.recv()
        return json.loads(message)


