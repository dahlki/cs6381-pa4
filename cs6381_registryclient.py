import random
import threading

import zmq
import json
import cs6381_constants as constants
import time

from cs6381_history import History
from cs6381_zkwatcher import Watcher
from cs6381_zkclient import ZooClient


class Registry:

    def __init__(self, role, address="localhost", port=None, strategy=None, client=None, registry_ip="10.0.0.1", replica=1):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket_registry_data = self.context.socket(zmq.SUB)
        self.cache_socket = self.context.socket(zmq.REQ)
        self.poller = zmq.Poller()

        self.topics = None

        self.role = role
        self.topics = None
        self.history = None
        self.address = address
        self.port = port
        self.strategy = strategy
        self.history_options = [0, 5, 10, 20, 30]
        self.history_topic_options = {}
        self.cache = History()
        self.registry_server_ip = "localhost" if self.address == "localhost" else None
        self.client = client
        self.should_start = True

        self.num_pubs = 1
        self.num_subs = 1
        self.num_brokers = 1
        self.num_registries = 1
        self.topo = "linear"

        self.registry_ips = []
        self.topic_thread = None
        self.lock = threading.Lock()

        self.zoo_client = ZooClient(role, self.address, self.port)
        self.zk = self.zoo_client.get_zk()

        self.zoo_client.get_watcher(constants.KAZOO_REGISTRIES_PATH, self.get_registry_ip, True)

        self.topic_registry = {}
        self.replica_count_list = []
        self.replica_num = replica

        self.message_history = None

        self.one = ["weather", "humidity", "airquality"]
        self.two = ["light", "pressure", "temperature"]
        self.three = ["sound", "altitude", "location"]

        self.join_election(replica)

        self.leader_pub = f"{self.address}:{self.port}"

    def join_election(self, replica_num):
        print(replica_num)
        if self.role == constants.BROKER:
            num = self.get_string_num(replica_num)
            self.zoo_client.join_broker_election(num)
            self.zoo_client.register(constants.KAZOO_BROKERS_PATH, f"{self.address}:{self.port}")
        # self.get_watcher(constants.KAZOO_BROKER_PATH, self.broker_watcher, False)

    def get_watcher(self, path, callback, do_watch_children):
        watcher = Watcher(self.zk, constants.REGISTRY, path)
        watcher.watch(callback, do_watch_children)
        return watcher

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
        self.socket.send_string('{} {} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port, "0"))

        try:
            message = self.socket.recv_string()
            if message:
                print("registry response received for register service: %s" % message)
                print("registered broker: {}\n".format(self.client))
                broker_list = json.loads(message)

                if broker_list:
                    print("GOT BROKER LIST FROM REGISTRY:")
                    print(broker_list)

                self.zoo_client.get_watcher("/publisher", self.pub_topic_watcher_callback, True)
                self.zoo_client.get_watcher("/subscriber", self.sub_topic_watcher_callback, True)
                self.client.start()

        except Exception:
            print("Exception occurred while registring broker!")
            raise

    def broker_watcher(self, path, data):
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
        self.get_topic_history_watcher(topic)

        if "pub" not in self.topic_registry:
            self.topic_registry["pub"] = {}

        if data:
            address = data.decode()
            self.topic_registry["pub"][topic] = address
        else:
            self.topic_registry["pub"][topic] = {}

        print(f"topic registry: {self.topic_registry}\n&&&&&&&&&&&&&&&&&")

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
            self.topic_registry["sub"] = {}

        if data:
            address = data.decode()
            self.topic_registry["sub"][topic] = address
        else:
            self.topic_registry["sub"][topic] = {}

        print(f"topic registry: {self.topic_registry}\n&&&&&&&&&&&&&&&&")

    def pub_callback(self, path, data):
        print("in pub callback...path: {} - data: {}".format(path, data))
        if data is not None:
            data = data.decode()
            if ":" in data:
                ip, port = data.split(":")
                print(self.leader_pub == f"{self.address}:{self.port}")
                if constants.KAZOO_BROKER_PATH in path and (self.leader_pub == f"{self.address}:{self.port}"):
                    self.client.stop()
                    self.client.start(f"{ip}:{self.port}")
                    print(f"pub starting on: {ip}:{self.port}")
        print(f"{path}-{data}\n")

# PUBLISHER #########################################################

    def register_publisher(self, topics):
        cache_thread = threading.Thread(target=self.set_history)
        # cache_thread.setDaemon(True)
        cache_thread.start()
        # send registration info to registry server
        print('register {} {} {} {}'.format(self.role, self.address, self.port, topics))
        self.socket.send_string('{} {} {} {} {}'.format(constants.REGISTER, self.role, self.address, self.port, self.history),
                                0 | zmq.SNDMORE)
        self.socket.send_json(json.dumps(topics), 1 | 0)

        # response from registry server
        message = self.socket.recv_string(0)
        if message:
            print("registry response received for register service: %s" % message)
            self.zoo_client.join_election()
            for topic in topics:

                self.zoo_client.register_topic_and_history("publisher", topic, self.history)

                self.zoo_client.register_topic("publisher", topic)
                self.zoo_client.get_watcher(f"/{topic}", None)
                self.zoo_client.get_watcher(f"/{topic}/{self.history}", self.pub_topic_history_watcher_callback)

                if self.strategy == constants.BROKER:
                    print("Watch Broker App!")
                    num = self.get_string_num_from_topic(topic)
                    self.zoo_client.get_watcher(f"{constants.KAZOO_BROKER_PATH}_{num}", self.pub_callback)
                    # self.zoo_client.get_watcher(constants.KAZOO_BROKER_PATH, self.pub_callback)
                else:
                    self.client.start()

        else:
            print("error - publisher not registered!")

    def pub_topic_history_watcher_callback(self, path, data):
        print("HELLOOOOOOOOOOOOOOOOOOOOOOOOO")
        print(path, data)
        topic, history = path.split("/")[1:]
        if data:
            print(topic, history, data)
            self.leader_pub = data.decode()

# END PUBLISHER #########################################################

    def sub_callback(self, path, data):
        print("in sub callback...")
        print(path, data)
        if data is not None:
            data = data.decode()
            if data:
                ip, port = data.split(":")
                if constants.KAZOO_BROKER_PATH in path:
                    self.client.connect('tcp://{}:{}'.format(ip, self.port))
                    print(f"sub starting on: {ip}:{self.port}")
        print(f"{path}-{data}\n")

    def get_topic_history_watcher(self, topic):
        for history in self.history_options:
            self.zoo_client.get_watcher(f"/{topic}/{history}", self.topic_history_callback)

    def topic_history_callback(self, path, data):
        print("topic history callback..................")

        topic, history = path.split("/")[1:]
        print(topic, history, data)
        if topic not in self.history_topic_options:
            self.history_topic_options[topic] = {}
        if data is not None:
            data = data.decode()
            print(data)
            self.history_topic_options[topic][history] = data
        else:
            self.history_topic_options[topic][history] = {}

        print(self.history_topic_options)

        if self.strategy == constants.DIRECT:
            if topic in self.history_topic_options:
                for history_num in self.history_options:
                    if history_num >= self.history:
                        if str(history_num) in self.history_topic_options[topic] and self.history_topic_options[topic][str(history_num)]:
                            # self.client.stop()
                            print("sending SUB history!!!")

                            print(self.history_topic_options)
                            print(self.history_topic_options[topic][str(history_num)])

                            cache_thread = threading.Thread(target=self.get_history, args=(constants.SUB, f"{self.history_topic_options[topic][str(history_num)]}", topic, self.history))
                            cache_thread.setDaemon(True)
                            cache_thread.start()
                            cache_thread.join()
                            print(self.message_history)
                            if self.message_history != "no history":
                                self.client.send_history(json.loads(self.message_history))
                            time.sleep(1)

                            self.client.connect(f"tcp://{self.history_topic_options[topic][str(history_num)]}")
                            self.client.subscribe(topic)
                            self.client.start()
                            break
        else:
            if self.role == constants.SUB:
                if topic in self.history_topic_options:
                    for history_num in self.history_options:
                        if history_num >= self.history:
                            if str(history_num) in self.history_topic_options[topic] and self.history_topic_options[topic][str(history_num)]:
                                print("sending SUB history!!!")

                                cache_thread = threading.Thread(target=self.get_history, args=(
                                constants.SUB, f"{self.history_topic_options[topic][str(history_num)]}", topic,
                                self.history))
                                cache_thread.setDaemon(True)
                                cache_thread.start()
                                cache_thread.join()
                                print(self.message_history)
                                if self.message_history != "no history":
                                    self.client.send_history(json.loads(self.message_history))
                                time.sleep(1)

    # SUBCRIBER #########################################################

    def register_subscriber(self, topics):

        self.topics = topics
        self.zoo_client.join_election()
        for topic in topics:
            self.zoo_client.register_topic("subscriber", topic)
            self.get_topic_history_watcher(topic)

        for topic in topics:
            if self.strategy == constants.DIRECT:
                # if topic in registry:
                if topic in self.history_topic_options:
                    for history_num in self.history_options:
                        if history_num >= self.history:
                            if str(history_num) in self.history_topic_options[topic] and self.history_topic_options[topic][str(history_num)]:
                                print("REGISTERING SUB!!!")
                                self.client.connect(f"tcp://{self.history_topic_options[topic][str(history_num)]}")
                                self.client.subscribe(topic)
            elif self.strategy == constants.BROKER:
                if topic in self.one:
                    num = "one"
                elif topic in self.two:
                    num = "two"
                else:
                    num = "three"
                self.zoo_client.get_watcher(f"{constants.KAZOO_BROKER_PATH}_{num}", self.sub_callback)
                self.client.subscribe(topic)

        self.client.start()

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

    def get_history(self, client, address, topic, history):
        print(type(address))
        if len(address) > 2:
            self.poller.register(self.cache_socket, zmq.POLLIN)
            self.cache_socket.connect('tcp://{}:{}'.format(self.registry_server_ip, constants.CACHE_PORT_NUMBER))
            print("getting history from registry................")
            self.cache_socket.send_string('{} {} {} {} {}'.format(constants.HISTORY, client, address, topic, history))

            message = self.cache_socket.recv()
            self.message_history = message.decode()
            print("MESSAGESSSSSSSS")
            print(self.message_history)
            return self.message_history

    def set_history(self):
        print("start history")
        self.poller.register(self.cache_socket, zmq.POLLIN)
        self.cache_socket.connect('tcp://{}:{}'.format(self.registry_server_ip, constants.CACHE_PORT_NUMBER))
        while True:
            for topic in self.topics:
                history = self.client.pub_history(topic)
                # print("GOT HISTORY!!!!!!!!!!!", history)
                if history:
                    self.cache_socket.send_string(f"{constants.HISTORY} {constants.PUB} {self.address}:{self.port} {topic} {self.history}", 0 | zmq.SNDMORE)
                    self.cache_socket.send_json(json.dumps(history), 1 | 0)
                    message = self.cache_socket.recv_string(0)
            time.sleep(5)

    def get_string_num(self, int_num):
        if int_num == 1:
            return "one"
        elif int_num == 2:
            return "two"
        else:
            return "three"

    def get_string_num_from_topic(self, topic):
        if topic in self.one:
            return "one"
        elif topic in self.two:
            return "two"
        else:
            return "three"

