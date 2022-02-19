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
import zmq
import uuid


# define an abstract base class for the publisher
import cs6381_util


class Publisher(object):

    def __init__(self, address, port, strategy):
        self.context = zmq.Context()
        self.address = address
        self.port = port
        self.strategy = strategy
        self.callback = None
        self.uuid = cs6381_util.create_uuid()

    @staticmethod
    def get_publisher_instance(self):
        if self.strategy == "direct":
            return DirectPublisher(self.ipaddr, self.port, self.strategy)
        elif self.strategy == "broker":
            return ViaBrokerPublisher(self.ipaddr, self.port, self.strategy)

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
    def start(self):
        pass

    @staticmethod
    def register_callback(self, cb):
        self.callback = cb

# a concrete class that disseminates info directly
class DirectPublisher(Publisher):

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, address, port, strategy):
        super().__init__(address, port, strategy)
        self.socket = self.context.socket(zmq.PUB)
        self.address = address
        self.port = port
        self.strategy = strategy

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        message = cs6381_util.get_publish_message(topic, value, self.address, self.uuid)
        self.socket.send_string(message)

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    def start(self):
        connection = 'tcp://*:{}'.format(self.port)
        print("I am the DirectPublisher's start method. binding to: {}".format(connection))
        self.socket.bind(connection)


# A concrete class that disseminates info via the broker
class ViaBrokerPublisher(Publisher):

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, address, port, strategy):
        super().__init__(address, port, strategy)
        self.socket = self.context.socket(zmq.PUB)
        self.address = address
        self.port = port

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        message = cs6381_util.get_publish_message(topic, value, self.address, self.uuid)
        self.socket.send_string(message)

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    def start(self, broker_ip):
        connection = 'tcp://{}:{}'.format(broker_ip, self.port)
        print("I am the send ViaBrokerPublisher's start method. connecting to {}".format(connection))
        self.socket.connect(connection)

