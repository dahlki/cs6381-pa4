###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the publisher proxy in the middleware layer
#
# Created: Spring 2022
#
###############################################

# A proxy for the publisher will be used in a remote procedure call
# approach.  We envision its use on the broker side when
# it delegates the work to the proxy to talk to its real counterpart. 
# One may completely avoid this approach if pure message passing is
# going to be used and not have a higher level remote procedure call approach.
import sys
from abc import ABC
from cs6381_publisher import DirectPublisher
from cs6381_publisher import ViaBrokerPublisher


class PublisherProxy:

    def __init__(self, strategy, port):
        self.strategy = strategy
        # self.address = address
        self.port = port
        pass

    def get_publisher(self):
        if self.strategy == "broker":
            print('creating ViaBrokerPublisher')
            return ViaBrokerPublisher(self.port)
        else:
            # return DirectPublisher()
            pass