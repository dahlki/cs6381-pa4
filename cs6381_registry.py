import zmq
import json
import cs6381_constants as constants
import time
import argparse
import threading


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Registry")
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")
    parser.add_argument("-p", "--publishers", type=int, default=1, help="number of publishers")
    parser.add_argument("-s", "--subscribers", type=int, default=1, help="number of subscribers")

    return parser.parse_args()


class Registry:

    def __init__(self, role, address="localhost", port=None, strategy=None, client=None):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, 1000)  # set timeout of 1 second

        self.socket_pull = self.context.socket(zmq.PULL)
        self.poller = zmq.Poller()

        self.role = role
        self.address = address
        self.port = port
        self.strategy = strategy
        self.serverIP = "localhost" if self.address == "localhost" else "10.0.0.1"
        self.client = client
        self.should_start = True
        self.connect_server()

        self.num_pubs = None
        self.num_subs = None

    def kickstart(self):
        while not self.should_start:
            pass

    def get_start_status(self):
        self.socket_pull.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PUSH_PORT))
        self.poller.register(self.socket_pull, zmq.POLLIN)
        print("checking start")
        # polls server to see if ready to start
        while not self.should_start:
            event = dict(self.poller.poll(1000))
            if self.socket_pull in event and event[self.socket_pull] == zmq.POLLIN:
                message = self.socket_pull.recv_string()
                # print("message received for start status: %s" % message)
                if message.startswith("start"):
                    self.should_start = True
                    msg, self.num_pubs, self.num_subs = message.split()
                elif message.startswith("wait"):
                    msg, remaining_pubs, remaining_subs, remaining_broker = message.split()
                    print("waiting to start...need {} pubs, {} subs, {} broker".format(remaining_pubs, remaining_subs,
                                                                                       remaining_broker))
            # time.sleep(1)

    def connect_server(self):
        #  connect to Registry Server
        print("connecting to registry server at {} {}".format(self.serverIP, constants.REGISTRY_PORT))
        self.socket.connect('tcp://{}:{}'.format(self.serverIP, constants.REGISTRY_PORT))

        # thread = threading.Thread(target=self.get_start_status)
        # thread.setDaemon(True)
        # thread.start()

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
                self.kickstart()
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
                    self.kickstart()
                    self.client.start(broker_ip)
                else:
                    broker_ip = None
                    print("broker ip not received for broker dissemination strategy!")
                    while broker_ip is None:
                        broker_ip = self.get_topic_connection(constants.BROKER_IP)
                        time.sleep(1)
                    print("broker's ip: %s" % broker_ip)
                    self.kickstart()
                    self.client.start(broker_ip)
            else:
                self.kickstart()
                self.client.start()

            print("registered publisher: {}".format(self.client))
        else:
            print("error - publisher not registered!")

    def register_subscriber(self, topics):

        registry = self.get_registry(constants.SUB)
        print("REGISTRY:")
        print(registry)
        if "pubNums" in registry and "subNums" in registry:
            self.num_pubs = registry["pubNums"]
            self.num_subs = registry["subNums"]
        while len(registry) <= 2:
            registry = self.get_registry(constants.SUB)
        for topic in topics:
            if self.strategy == constants.DIRECT:
                if topic in registry:
                    for connection in registry[topic]:
                        print("in registry - subscriber attempting to connect to %s for topic %s" % (connection, topic))
                        self.client.connect(connection)
                        self.client.subscribe(topic)
            elif self.strategy == constants.BROKER:
                if constants.BROKER_IP in registry:
                    self.client.connect('tcp://{}:{}'.format(registry[constants.BROKER_IP], self.port))
                    self.client.subscribe(topic)
                else:
                    print("Must start Broker App first!")
                    broker_ip = None
                    print("broker ip not received for broker dissemination strategy!")
                    while broker_ip is None:
                        broker_ip = self.get_topic_connection(constants.BROKER_IP)
                    print("broker's ip: %s" % broker_ip)
                    self.client.connect('tcp://{}:{}'.format(broker_ip, self.port))
                    self.client.subscribe(topic)
        self.kickstart()
        self.client.start(self.num_pubs, self.num_subs, self.strategy)

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
        if message:
            return json.loads(message)


class RegistryServer:

    def __init__(self, strategy, pubs, subs):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket_push = self.context.socket(zmq.PUSH)
        self.registry = {}
        self.strategy = strategy
        self.broker = 1 if strategy == constants.BROKER else 0
        self.pubs = pubs
        self.subs = subs
        self.wait = True

    def discover(self, topic):
        return self.registry.get(topic, None)

    def should_start(self):
        pubs = self.pubs
        subs = self.subs
        broker = self.broker
        print("need to register {} pubs, {} subs, {} broker".format(pubs, subs, broker))
        while True:
            if self.broker <= 0 and self.pubs <= 0 and self.subs <= 0:
                self.wait = False
                self.socket_push.send_string("start {} {}".format(pubs, subs))
            else:
                self.socket_push.send_string("wait {} {} {}".format(self.pubs, self.subs, self.broker))
            time.sleep(1)

    def broker_registration(self, address, port):
        self.registry[constants.BROKER_IP] = address
        self.registry["brokerPort"] = port
        # self.broker -= 1
        self.socket.send_string("successfully registered broker's address {}!".format(address))

    def pub_registration(self, address, port, topics):
        connection = 'tcp://{}:{}'.format(address, port)

        # load topics into registry
        for topic in json.loads(topics):
            print("inserting pub topic into registry: {}, address: {}".format(topic, connection))
            if topic in self.registry:
                if connection not in self.registry[topic]:
                    self.registry[topic].append(connection)
            else:
                self.registry[topic] = [connection]

        print("REGISTRY: {}".format(self.registry))
        # self.pubs -= 1

        if constants.BROKER_IP in self.registry and self.strategy == constants.BROKER:
            broker_ip = self.registry[constants.BROKER_IP]
            self.socket.send_string("successfully registered pub for {} at {}!".format(topics, connection),
                                    0 | zmq.SNDMORE)
            self.socket.send_string(broker_ip, 1 | 0)
        else:
            self.socket.send_string(
                "successfully registered pub for {} at {}!".format(topics, connection))

    def start_receiving(self):
        self.registry["pubNums"] = self.pubs
        self.registry["subNums"] = self.subs
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

                    if role == constants.PUB:
                        # create pub connection string for topic registration
                        self.pub_registration(address, port, topics)

                    if role == constants.SUB:
                        # self.subs -= 1
                        self.socket.send_string("successfully registered sub!")

                elif action == constants.DISCOVER:
                    topic = info[0]
                    print('retrieving address for: {}'.format(topic))
                    if topic:
                        address = self.discover(topic)
                        print(address)
                        self.socket.send_string(json.dumps(address))

                elif action == constants.REGISTRY:
                    # if info[0] == "sub":
                        # self.subs -= 1
                    self.socket.send_string(json.dumps(self.registry))
                    print("registry sent")

    def start(self):
        print("starting registry server on port: {}".format(constants.REGISTRY_PORT))

        self.socket.bind('tcp://*:{}'.format(constants.REGISTRY_PORT))
        self.socket_push.bind('tcp://*:{}'.format(constants.REGISTRY_PUSH_PORT))

        # thread_kickstart = threading.Thread(target=self.should_start)
        # thread_kickstart.setDaemon(True)
        # thread_kickstart.start()

        thread_req = threading.Thread(target=self.start_receiving())
        thread_req.setDaemon(True)
        thread_req.start()


def main():
    args = parseCmdLineArgs()
    registry = RegistryServer(args.disseminate, args.publishers, args.subscribers)
    registry.start()


if __name__ == "__main__":
    main()
