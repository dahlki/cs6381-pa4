###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton code for the publisher application
#
# Created: Spring 2022
#
###############################################


# The core logic of the publisher application will be as follows
# (1) The publisher app decides which all topics it is going to publish.
# We don't care for the values for the topic being published (any
# arbitrary value for that topic is fine)
#
# (2) the application obtains a handle to the broker (which under the
# hood will be a proxy object but the application doesn't know it is a
# proxy).  Moreover, here I am assuming that the lookup and broker are
# lumped together. Else you need to get the lookup service and then
# ask the lookup service to send a representation of the broker
#
# (3) Register with the lookup/broker letting it know all the topics it is publishing
# and any other details needed for the underlying middleware to make the
# communication happen via ZMQ
#
# (4) Obtain a handle to the publisher object (which maybe a specialized
# object depending on whether it is using the direct dissemination or
# via broker approach)
#
# (5) Wait for a kickstart msg from the broker. This is when the publisher knows
# that all subscribers are ready and that it is time for publishers to start
# publishing.
#
# (6) Start a loop on the publisher object for sending of topics
#
#       In each iteration, the app decides (randomly) on which all
#       topics it is going to publish, and then accordingly invokes
#       the publish method of the publisher object passing the topic
#       and its value. 
#
#       Note that additional info like timestamp etc may also need to
#       be sent and the whole thing serialized under the hood before
#       actually sending it out.
#
#
# (6) When the loop terminates, possibly after a certain number of
# iterations of publishing are over, proceed to clean up the objects and exit
#

import argparse  # for argument parsing

from cs6381_configurator import Configurator  # factory class
from cs6381_registryclient import Registry
from cs6381_util import get_system_address
from cs6381_util import get_timestamp
import cs6381_constants as constants
import time
import asyncio

# import any other packages you need.


###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Publisher Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    # Here I am showing one example of adding a command line
    # arg for the dissemination strategy. Feel free to modify. Add more
    # options for all the things you need.
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")
    # parser.add_argument("-a", "--ipaddr", type=str, default='localhost', help="address")
    parser.add_argument("-p", "--port", type=int, default=constants.PUBLISHER_PORT_NUMBER,
                        help="specify port number; default is 5555")
    parser.add_argument("-n", "--number", type=int, choices=range(1, 9), default=None,
                        help="number of topics to publish; between 1 and 8; default will be random number of topics")
    parser.add_argument("-t", "--time", type=float, default=.05,
                        help="specify seconds between publishing messages; default is 0.05")
    parser.add_argument("-i", "--registryIP", type=str, help="IP address of any existing Registry node")
    parser.add_argument("-m", "--history", type=int, choices=[0, 5, 10, 20, 30], default=0, help="max number of history messages; range from 0 - 20; default = 0")


    return parser.parse_args()


# def start_publishing():
#     print("CALLBACK called...")


###################################
#
# Main program
#
###################################
async def main():
    # first parse the arguments
    print("Main: parse command line arguments")
    args = parseCmdLineArgs()
    print(args)
    ip = get_system_address()
    print("publisher ip address: %s" % ip)

    # get hold of the configurator, which is the factory that produces
    # many kinds of artifacts for us
    config = Configurator(args, ip, args.history)

    # Ask the configurator to gßive us a random subset of topics that we can publish
    my_topics = config.get_interest(args.number)
    print("Publisher interested in publishing on these topics: {}".format(my_topics))

    # get a handle to our publisher object
    pub = config.get_publisher()
    print("created PUBLISHER: ", pub)

    # get a handle to our broker object (will be a proxy)
    registry = Registry(constants.PUB, ip, args.port, args.disseminate, pub, args.registryIP)
    registry.register(my_topics, args.history)
    # wait for kickstart event from broker
    # now do the publication for as many iterations that we plan to do
    print("pubapp publishing...")
    while True:
        for topic in my_topics:
            value = config.get_topic_message(topic)
            pub.publish(topic, value)
            time.sleep(args.time)  # pause between messages


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    asyncio.run(main())
