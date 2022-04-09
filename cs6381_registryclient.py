import random
import threading

import zmq
import json
import cs6381_constants as constants
import time

from cs6381_zkelection import Election
from cs6381_zkwatcher import Watcher
from cs6391_zkclient import ZooClient


class Registry:

    def __init__(self, role, address="localhost", port=None, strategy=None, client=None, registry_ip="10.0.0.1"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        # self.socket.setsockopt(zmq.RCVTIMEO, 1000)  # set timeout of 1 seconds
        self.socket_should_start = self.context.socket(zmq.SUB)
        self.socket_registry_data = self.context.socket(zmq.SUB)
        self.poller = zmq.Poller()

        self.role = role
        self.address = address
        self.port = port
        self.strategy = strategy
        self.serverIP = "localhost" if self.address == "localhost" else None
        self.client = client
        self.should_start = True

        self.num_pubs = None
        self.num_subs = None
        self.num_registries = None
        self.topo = None

        self.registry_ips = []
        self.lock = threading.Lock()

        self.zoo_client = ZooClient(role, self.address, self.port)
        self.zoo_client.join_election()
        self.zk = self.zoo_client.get_zk()

        self.get_watcher(constants.KAZOO_REGISTRIES_PATH, self.get_registry_ip, True)

    def get_registry_ip(self, path, children):
        if self.serverIP is None:
            server_ip = random.choice(children).split(":")[0]
            self.serverIP = server_ip
            self.connect_server()
        elif any(self.serverIP in child for child in children):
            pass
        else:
            server_ip = random.choice(children).split(":")[0]
            self.serverIP = server_ip
            self.connect_server()
        print("registries children: {}".format(children))
        print("connected to new registry server at: {}".format(self.serverIP))

    def registry_heartbeat(self):
        self.socket.send_string("heartbeat")
        events = dict(self.poller.poll(1000))

        if self.socket in events and events[self.socket] == zmq.POLLIN:
            message = self.socket.recv_string()

            if message and message == "beat":
                print("RECEIVED HEARTBEAT!!!")
                return True
        return False

    def get_new_registry_data(self, topics):
        registry_info = [constants.REGISTRY_NODES, constants.BROKER_IP, "start"]
        if topics is not None:
            registry_info.extend(topics)

        self.socket_registry_data.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PUB_PORT_NUMBER))
        self.registry_ips.append(self.serverIP)

        for meta_data in registry_info:
            print("subscribing to: {}".format(meta_data))
            self.socket_registry_data.setsockopt_string(zmq.SUBSCRIBE, meta_data)

        self.poller.register(self.socket_registry_data, zmq.POLLIN)
        while True:
            events = dict(self.poller.poll(1000))
            if self.socket_registry_data in events and events[self.socket_registry_data] == zmq.POLLIN:
                print("RECEIVED MESSAGE!!!")
                message = self.socket_registry_data.recv_string()
                print("meta data message: ", message)
                topic, connection = message.split(" ", 1)
                if topic.startswith(constants.REGISTRY_NODES):
                    connection = json.loads(connection)
                    # print("registry client received META data for registry node:", connection)
                    for ip in connection:
                        if not(ip in self.registry_ips):
                            print("new ip", topic, ip)
                            self.socket_registry_data.connect('tcp://{}:{}'.format(ip, constants.REGISTRY_PUB_PORT_NUMBER))
                            self.registry_ips.append(ip)
                else:
                    print("registry client received META data for topic:", topic, connection)
                    if self.strategy == constants.DIRECT and connection.startswith("tcp"):
                        self.client.connect(connection)
                        self.client.subscribe(topic)

    def connect_server(self):
        #  connect to Registry Server
        print("connecting to registry server at {} {}\n".format(self.serverIP, constants.REGISTRY_PORT_NUMBER))
        self.socket.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PORT_NUMBER))

    def get_watcher(self, path, callback, watch_children=False):
        watcher = Watcher(self.zk, self.role, path)
        watcher.watch(callback, watch_children)
        return watcher

    def register(self, topics=None):
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
                print("registered broker: {}".format(self.client))
                self.get_watcher(constants.KAZOO_BROKER_PATH, None)
                self.client.start()
        except Exception:
            print("must start Registry first!")
            raise

    def pub_callback(self, path, data):
        print("in pub callback...path: {} - data: {}".format(path, data))
        if data is not None:
            data = data.decode()
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
            if self.strategy == constants.BROKER:
                self.get_watcher(constants.KAZOO_BROKER_PATH, self.pub_callback)
            else:
                self.client.start()
        else:
            print("error - publisher not registered!")

    def sub_callback(self, path, data):
        print("in sub callback...")
        if data is not None:
            data = data.decode()
            ip, port = data.split(":")

            if constants.KAZOO_BROKER_PATH in path:
                self.client.connect('tcp://{}:{}'.format(ip, self.port))
        print(f"{path}-{data}\n")

    def register_subscriber(self, topics):
        registry = None
        thread = threading.Thread(target=self.get_new_registry_data, args=(topics,))
        thread.setDaemon(True)
        thread.start()

        self.socket.send_string('{} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port))
        message = self.socket.recv_string(0)
        # success, topo, pubs, subs, registries = message.split(" ")
        # # used only for data file name creation
        # if success == "success":
        #     self.topo = topo
        #     self.num_pubs = pubs
        #     self.num_subs = subs
        #     self.num_registries = registries

        if message:
            print(message)
            while registry is None or len(registry) <= 0:
                registry = self.get_registry(constants.SUB)
                if registry is not None and registry["nodes"]:
                    self.registry_ips = registry["nodes"]
                    for ip in self.registry_ips:
                        self.socket_registry_data.connect('tcp://{}:{}'.format(ip, constants.REGISTRY_PUB_PORT_NUMBER))
                time.sleep(2)
        print("REGISTRY:")
        print(registry)

        if self.strategy == constants.BROKER:
            print("Must start Broker App first!")
            self.get_watcher(constants.KAZOO_BROKER_PATH, self.sub_callback)

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

        self.client.start(self.num_pubs, self.num_subs, self.num_registries, self.strategy, self.topo)

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

    def get_broker_ip(self):
        broker_ip = None
        print("broker ip not received for broker dissemination strategy!")
        while broker_ip is None:
            broker_ip = self.get_topic_connection(constants.BROKER_IP)
        print("broker's ip: %s" % broker_ip)
        return broker_ip

