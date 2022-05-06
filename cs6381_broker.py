###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# See the cs6381_publisher.py file for how an abstract Publisher class is
# defined and then two specialized classes. We may need similar things here.
# I am also assuming that discovery and dissemination are lumped into the
# broker. Otherwise keep them in separate files.
import json
import threading

import zmq
from abc import abstractmethod
import cs6381_constants as constants
from cs6381_zkelection import Election
from cs6381_util import get_system_address
from cs6381_zkwatcher import Watcher


class Broker(object):

    def __init__(self, address='localhost', port=constants.BROKER_PORT_NUMBER, strategy="direct"):
        self.context = zmq.Context()
        self.strategy = strategy
        self.address = address
        self.port = port

    @staticmethod
    def get_broker_instance(self, broker_num):
        if self.strategy == "broker":
            return ViaBroker(broker_num, self.ipaddr, self.port, self.strategy)

    @abstractmethod
    def start(self):
        pass


class ViaBroker(Broker):

    def __init__(self, broker_num, strategy='direct', address='localhost', port=constants.BROKER_PORT_NUMBER):
        super().__init__(strategy, address, port)
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.pub_port = constants.PUBLISHER_PORT_NUMBER
        self.sub_port = constants.SUBSCRIBER_PORT_NUMBER
        self.xpub = self.context.socket(zmq.XPUB)
        self.xsub = self.context.socket(zmq.XSUB)

        self.iterations = None
        self.broker_num = broker_num
        self.one = ["weather", "humidity", "airquality"]
        self.two = ["light", "pressure", "temperature"]
        self.three = ["sound", "altitude", "location"]
        self.topics = self.get_topic_list()

    def get_topic_list(self):
        if self.broker_num == 1:
            return self.one
        elif self.broker_num == 2:
            return self.two
        else:
            return self.three

    def start(self):

        # create xpub
        xsub_address = 'tcp://*:{}'.format(self.sub_port)
        self.xpub.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.xpub.bind(xsub_address)

        # create xsub
        xpub_address = 'tcp://*:{}'.format(self.pub_port)
        self.xsub.bind(xpub_address)

        # register with poller
        self.poller.register(self.xpub, zmq.POLLIN)
        self.poller.register(self.xsub, zmq.POLLIN)

        print('ViaBroker starting')
        while self.iterations is None or self.iterations > 0:
            event = dict(self.poller.poll())
            if self.xpub in event:
                msg = self.xpub.recv_multipart()
                for topic in self.topics:
                    if msg[0] and topic in msg[0].decode():
                        print("from subscription: %r" % msg)
                        self.xsub.send_multipart(msg)
            if self.xsub in event:
                msg = self.xsub.recv_multipart()
                for topic in self.topics:
                    if msg[0] and topic in msg[0].decode():
                        print("from publisher: %r" % msg)
                        self.xpub.send_multipart(msg)
            if self.iterations:
                self.iterations -= 1
