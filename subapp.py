###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton logic for the subscriber application
#
# Created: Spring 2022
#
###############################################


# The basic logic of the subscriber application will be as follows (see pubapp.py
# for additional details.
#
# (1) The subscriber app decides which all topics it is going to subscriber to.
# (see pubapp.py file for how the publisher gets its interest randomly chosen
# for it.
#
# (2) the application obtains a handle to the broker (which under the
# hood will be a proxy object but the application doesn't know it is a
# proxy).
#
# (3) Register with the broker letting it know all the topics it is interested in
# and any other details needed for the underlying middleware to make the
# communication happen via ZMQ
#
# (4) Obtain a handle to the subscriber object (which maybe a specialized
# object depending on whether it is using the direct dissemination or
# via broker)
#
# (5) Wait for the broker to let us know who our publishers are for the
# topics we are interested in. In the via broker approach, the broker is the
# only publisher for us.
#
#
# (5) Have a receiving loop. See scaffolding code for polling where
# we show how different ZMQ sockets can be polled whenever there is
# an incoming event.
#
#    In each iteration, handle all the events that have been enabled by
#    receiving the data. Get the timestamp and obtain the latency for each
#    received topic from each publisher and store this info possibly in a
#    time series database like InfluxDB
#
# (6) if you have logic that allows this forever loop to terminate, then
# go ahead and clean up everything.

import argparse   # for argument parsing
from cs6381_configurator import Configurator
from cs6381_util import get_system_address
from cs6381_registryclient import Registry
import cs6381_constants as constants
import asyncio

def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Subscriber Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    # Here I am showing one example of adding a command line
    # arg for the dissemination strategy. Feel free to modify. Add more
    # options for all the things you need.
    # parser.add_argument("-a", "--ipaddr", default="localhost", help="IP address of the message passing server, default: localhost")
    parser.add_argument ("-d", "--disseminate", choices=["direct", "broker"], default="direct", help="Dissemination strategy: direct or via broker; default is direct")
    parser.add_argument ("-p", "--port", type=int, default=constants.SUBSCRIBER_PORT_NUMBER, help="specify port number; default is 5556")
    parser.add_argument("-n", "--number", type=int, choices=range(1, 9), default=None,
                        help="number of topics to publish; between 1 and 8")
    parser.add_argument("-i", "--registryIP", type=str, help="IP address of any existing Registry node")

    return parser.parse_args()


async def main():
    print("Main: parse command line arguments")
    args = parseCmdLineArgs()
    print(args)
    ip = get_system_address()
    print(ip)
    print("subscriber ip address: %s" % ip)

    config = Configurator(args, ip)

    my_topics = config.get_interest(args.number)
    print("Subscriber interested in subscribing on these topics: {}".format(my_topics))

    sub = config.get_subscriber()
    print("created SUBSCRIBER: ", sub)
    sub.notify(my_topics, lambda x: print("app notify - {} \n".format(x)))

    registry = Registry(constants.SUB, ip, args.port, args.disseminate, sub, args.registryIP)
    registry.register(my_topics)


if __name__ == "__main__":
    asyncio.run(main())