###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the subscriber functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# Please see the corresponding hints in the cs6381_publisher.py file
# to see how an abstract class is defined and then two specialized classes
# are defined based on the dissemination approach. Something similar
# may have to be done here. If dissemination is direct, then each subscriber
# will have to connect to each separate publisher with whom we match.
# For the ViaBroker approach, the broker is our only publisher for everything.
import time
from abc import abstractmethod
import zmq
import cs6381_util


class Subscriber(object):
    def __init__(self, address, port, strategy):
        self.context = zmq.Context()
        self.address = address
        self.port = port
        self.strategy = strategy
        self.uuid = cs6381_util.create_uuid()
        self.iterations = 500
        self.topics = {}
        self.cb = None
        self.messages = {}

    @staticmethod
    def get_subscriber_instance(self):
        if self.strategy == "direct":
            return DirectSubscriber(self.ipaddr, self.port, self.strategy)
        elif self.strategy == "broker":
            return ViaBrokerSubscriber(self.ipaddr, self.port, self.strategy)

    @abstractmethod
    def subscribe(self, topic):
        pass

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing.
    @abstractmethod
    def start(self, num_pubs, num_subs, strategy):
        pass

    def notify(self, topics, cb):
        for topic in topics:
            self.topics.update({topic: topic})
        self.cb = cb


class DirectSubscriber(Subscriber):
    def __init__(self, address, port, strategy):
        super().__init__(address, port, strategy)
        self.socket = self.context.socket(zmq.SUB)
        self.poller = zmq.Poller()
        self.address = address
        self.port = port
        self.strategy = strategy

    def connect(self, connection_string):
        print("subscriber connecting to: {}".format(connection_string))
        self.socket.connect(connection_string)

    def subscribe(self, topic):
        print("DirectSubscriber subscribing to topic: ", topic)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def start(self, num_pubs, num_subs, strategy):
        print("subscriber starting event loop")
        self.poller.register(self.socket, zmq.POLLIN)
        while self.iterations > -1:
            events = dict(self.poller.poll(1000))
            if self.socket in events and events[self.socket] == zmq.POLLIN:
                message = self.socket.recv_string()
                data = cs6381_util.get_subscribe_message(message, self.address, self.uuid)
                topic, *value = data
                if topic in self.topics:
                    self.cb(data)
                self.iterations -= 1
        cs6381_util.write_to_csv(num_pubs, num_subs, strategy)


class ViaBrokerSubscriber(Subscriber):
    def __init__(self, address, port, strategy):
        super().__init__(address, port, strategy)
        self.socket = self.context.socket(zmq.SUB)
        self.poller = zmq.Poller()
        self.address = address
        self.port = port
        self.strategy = strategy

    def connect(self, connection_string):
        print("I am the send ViaBrokerSubscriber's connect method. connecting to {}".format(connection_string))
        self.socket.connect(connection_string)

    def subscribe(self, topic):
        print("ViaBrokerSubscriber subscribing to topic: ", topic)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def start(self, num_pubs, num_subs, strategy):
        print("subscriber starting event loop")
        # cs6381_util.get_output()
        self.poller.register(self.socket, zmq.POLLIN)
        while self.iterations > -1:
            # poll for events. We give it an infinite timeout.
            # The return value is a socket to event mask mapping
            events = dict(self.poller.poll(1000))
            if self.socket in events:
                message = self.socket.recv_string()
                data = cs6381_util.get_subscribe_message(message, self.address, self.uuid)
                topic, *value = data
                if topic in self.topics:
                    self.cb(data)
                self.iterations -= 1
        cs6381_util.write_to_csv(num_pubs, num_subs, strategy)
