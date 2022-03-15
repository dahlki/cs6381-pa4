import threading

import zmq
import json
import cs6381_constants as constants
import time


class Registry:

    def __init__(self, role, address="localhost", port=None, strategy=None, client=None, registry_ip="10.0.0.1"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        # self.socket.setsockopt(zmq.RCVTIMEO, 1000)  # set timeout of 1 seconds
        self.socket_should_start = self.context.socket(zmq.PAIR)
        self.socket_registry_data = self.context.socket(zmq.SUB)
        self.poller = zmq.Poller()

        self.role = role
        self.address = address
        self.port = port
        self.strategy = strategy
        self.serverIP = "localhost" if self.address == "localhost" else registry_ip
        self.client = client
        self.should_start = False
        self.connect_server()

        self.num_pubs = None
        self.num_subs = None
        self.topo = None

        self.registry_ips = []
        self.lock = threading.Lock()

    def get_new_registry_data(self, topics):
        registry_info = [constants.REGISTRY_NODES, "pubNums", "subNums", constants.BROKER_IP, "silvia"]
        if topics is not None:
            registry_info.extend(topics)

        self.socket_registry_data.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PUB_PORT_NUMBER))
        self.registry_ips.append(self.serverIP)

        for meta_data in registry_info:
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

    def get_start_status(self):
        # self.socket_should_start.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PUSH_PORT_NUMBER))
        # self.poller.register(self.socket_should_start, zmq.POLLIN)

        print("checking start")
        # polls server to see if ready to start
        while not self.should_start:
            event = dict(self.poller.poll(2000))
            if self.socket_should_start in event:
                message = self.socket_should_start.recv_string()
                print("message received for start status: %s" % message)
                if message.startswith("start"):
                    self.should_start = True
                    print("START!!!", message)

    def connect_registry(self):
        #  connect to Registry Server
        print("connecting to registry server at {} {}".format(self.serverIP, constants.REGISTRY_PORT_NUMBER))
        self.socket.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PORT_NUMBER))

    def connect_server(self):
        #  connect to Registry Server
        # print("connecting to registry server at {} {}".format(self.serverIP, constants.REGISTRY_PORT_NUMBER))
        # self.socket.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PORT_NUMBER))

        # self.socket_should_start.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PUSH_PORT_NUMBER))
        # self.poller.register(self.socket_should_start, zmq.POLLIN)

        thread_registry = threading.Thread(target=self.connect_registry)
        thread_registry.setDaemon(True)
        thread_registry.start()

        # thread_should_start = threading.Thread(target=self.get_start_status)
        # thread_should_start.setDaemon(True)
        # thread_should_start.start()

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
                self.client.start()
        except Exception:
            print("must start Registry first!")
            raise

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
                if self.socket.getsockopt(zmq.RCVMORE):
                    print("receiving brokerIP")
                    broker_ip = self.socket.recv_string(1)
                    print("broker's ip: %s" % broker_ip)
                else:
                    broker_ip = None
                    print("broker ip not received for broker dissemination strategy!")
                    while broker_ip is None:
                        broker_ip = self.get_topic_connection(constants.BROKER_IP)
                        time.sleep(1)
                    print("broker's ip: %s" % broker_ip)
                self.client.start(broker_ip)
            else:
                self.client.start()

            print("registered publisher: {}".format(self.client))
        else:
            print("error - publisher not registered!")

    def register_subscriber(self, topics):
        registry = None
        thread = threading.Thread(target=self.get_new_registry_data, args=(topics,))
        thread.setDaemon(True)
        thread.start()

        self.socket.send_string('{} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port))
        message = self.socket.recv_string(0)
        success, topo, pubs, subs = message.split(" ")
        if success == "success":
            self.topo = topo
            self.num_pubs = pubs
            self.num_subs = subs

        if message:
            print(message)
            while registry is None or len(registry) <= 1:
                if registry is not None and registry["nodes"]:
                    self.registry_ips = registry["nodes"]
                    for ip in self.registry_ips:
                        self.socket_registry_data.connect('tcp://{}:{}'.format(ip, constants.REGISTRY_PUB_PORT_NUMBER))

                registry = self.get_registry(constants.SUB)
                time.sleep(2)
        print("REGISTRY:")
        print(registry)

        for topic in topics:
            if self.strategy == constants.DIRECT:
                if topic in registry:
                    for connection in registry[topic]:
                        print(topic, registry[topic])
                        print("in registry - subscriber attempting to connect to %s for topic %s" % (connection, topic))
                        self.client.connect(connection)
                        self.client.subscribe(topic)
            elif self.strategy == constants.BROKER:
                print("Must start Broker App first!")
                broker_ip = None
                print("broker ip not received for broker dissemination strategy!")
                while broker_ip is None:
                    broker_ip = self.get_topic_connection(constants.BROKER_IP)
                print("broker's ip: %s" % broker_ip)
                self.client.connect('tcp://{}:{}'.format(broker_ip, self.port))
                self.client.subscribe(topic)

        # while not self.should_start:
        #     pass
        self.client.start(self.num_pubs, self.num_subs, self.strategy, self.topo)

        print("registered subscriber: {}".format(self.client))

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

