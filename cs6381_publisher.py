###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the middleware layer for the publisher functionality
#
# Created: Spring 2022
#
###############################################

# ABC stands for abstract base class and this is how Python library
# defines the underlying abstract base class
from abc import abstractmethod
from collections import deque

import zmq
import uuid

# define an abstract base class for the publisher
import cs6381_constants as constants
import cs6381_util
from cs6381_history import History
from cs6381_zkelection import Election
from cs6381_zkwatcher import Watcher


class Publisher(object):

    def __init__(self, address, port, strategy, history):
        self.context = zmq.Context()
        self.address = address
        self.port = port
        self.strategy = strategy
        self.callback = None
        self.uuid = cs6381_util.create_uuid()
        self.history = history

    @staticmethod
    def get_publisher_instance(self):
        if self.strategy == "direct":
            return DirectPublisher(self.ipaddr, self.port, self.strategy, self.history)
        elif self.strategy == "broker":
            return ViaBrokerPublisher(self.ipaddr, self.port, self.strategy, self.history)

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    @abstractmethod
    def publish(self, topic, value):
        pass

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    @abstractmethod
    def start(self, broker):
        pass

    @abstractmethod
    def stop(self):
        pass

    @staticmethod
    def register_callback(self, cb):
        self.callback = cb


# a concrete class that disseminates info directly
class DirectPublisher(Publisher):

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, address, port, strategy, history):
        super().__init__(address, port, strategy, history)
        self.socket = self.context.socket(zmq.PUB)
        self.address = address
        self.port = port
        self.address = f"{self.address}:{self.port}"
        self.strategy = strategy
        self.connection = None
        self.history_max = history
        self.cache = History()

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        print("publish", topic, value)
        message = cs6381_util.get_publish_message(topic, value, self.address, self.uuid)
        self.cache.register(self.address, self.history_max, topic, message)
        # self.cache.get_history(self.address, topic)
        self.socket.send_string(message)

    def pub_history(self, topic):
        return self.cache.get_history(self.address, topic)

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    def start(self, broker=None):
        self.connection = 'tcp://*:{}'.format(self.port)
        print("I am the DirectPublisher's start method. binding to: {}".format(self.connection))
        self.socket.bind(self.connection)

    def stop(self):
        self.socket.disconnect(self.connection)


# A concrete class that disseminates info via the broker
class ViaBrokerPublisher(Publisher):

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, address, port, strategy, history):
        super().__init__(address, port, strategy, history)
        self.socket = self.context.socket(zmq.PUB)
        self.address = address
        self.port = port
        self.address = f"{self.address}:{self.port}"
        self.connection = None
        self.history_max = history
        self.cache = History()

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        message = cs6381_util.get_publish_message(topic, value, self.address, self.uuid)
        self.cache.register(self.address, self.history_max, topic, message)
        # self.cache.get_history(self.address, topic)
        # print(message)
        self.socket.send_string(message)

    def pub_history(self, topic):
        return self.cache.get_history(self.address, topic)

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    def start(self, broker_ip):
        print(broker_ip)
        self.connection = 'tcp://{}'.format(broker_ip)
        print("I am the send ViaBrokerPublisher's start method. connecting to {}".format(self.connection))
        self.socket.connect(self.connection)

    def stop(self):
        if self.connection is not None:
            self.socket.disconnect(self.connection)
