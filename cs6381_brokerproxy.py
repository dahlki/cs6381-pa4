###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker proxy at the middleware layer
#
# Created: Spring 2022
#
###############################################

# If you decide to do the RPC approach, you might need a proxy for the
# real broker.
#
# This is the proxy object for the broker which is held by both the
# publisher and subscriber-side middleware to store info about the
# whereabouts of the actual broker. The application level logic does not
# know that it is talking to a proxy object. It will simply invoke methods
# on the proxy, which then get translated under the hood into the appropriate
# serialization logic and sending to the real broker

import zmq
from cs6381_broker import Broker


class BrokerProxy:

    def __init__(self, strategy='direct', role=None, address='localhost', port=None):
        self.context = zmq.Context
        self.directory = None
        self.proxy = None
        self.strategy = strategy
        self.address = address
        self.role = role
        self.port = port

    def register(self):
        Broker(self.strategy, self.role, self.address, self.port).start()
