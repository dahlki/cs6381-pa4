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

import zmq
from abc import abstractmethod
from cs6381_registry import Registry
import cs6381_constants as ports

class Broker(object):

    def __init__(self,  strategy="direct", address='localhost', port=ports.BROKER_PORT):
        self.context = zmq.Context()
        self.strategy = strategy
        self.address = address
        self.port = port

    @staticmethod
    def get_broker_instance(self):
        if self.strategy == "broker":
            return ViaBroker(self.ipaddr, self.port, self.strategy)

    @abstractmethod
    def start(self):
        pass


class ViaBroker(Broker):

    def __init__(self, strategy='direct', address='localhost', port=ports.BROKER_PORT):
        super().__init__(strategy, address, port)
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.pub_port = ports.PUBLISHER_PORT
        self.sub_port = ports.SUBSCRIBER_PORT
        self.xpub = self.context.socket(zmq.XPUB)
        self.xsub = self.context.socket(zmq.XSUB)

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
        while True:
            event = dict(self.poller.poll())
            if self.xpub in event:
                msg = self.xpub.recv_multipart()
                print("from subscription: %r" % msg)
                self.xsub.send_multipart(msg)
            if self.xsub in event:
                msg = self.xsub.recv_multipart()
                print("from publisher: %r" % msg)
                self.xpub.send_multipart(msg)