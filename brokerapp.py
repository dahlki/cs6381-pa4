###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton code for the broker application
#
# Created: Spring 2022
#
###############################################

# Note that here I am lumping the discovery and dissemination into a 
# single capability. You could decide to keep the two separate to make
# the code cleaner and extensible

# The basic logic of the broker application will be as follows
#
# (1) Obtain a handle to the specialized broker object (which
# works only as a lookup service for the Direct dissemination
# strategy or the one that also is involved in dissemination)
#
# (2) Do any initialization steps as needed
#
# (3) Start the broker's event loop so that it keeps running forever
# accepting events and handling them at the middleware layer
#
import argparse
from cs6381_registry import Registry
from cs6381_configurator import Configurator
from cs6381_util import get_system_address
import cs6381_constants as ports


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="broker", help="Dissemination strategy: direct or via broker; default is direct")
    # parser.add_argument("-a", "--ipaddr", type=str, default='localhost', help="address")
    parser.add_argument("-p", "--port", type=int, default=ports.BROKER_PORT, help="port number")

    return parser.parse_args()


def main():
    args = parseCmdLineArgs()
    print('command line arguments: ', args)
    ip = get_system_address()

    config = Configurator(args, ip)

    broker = config.get_broker()

    registry = Registry("broker", ip, args.port, args.disseminate, broker)
    registry.register()


if __name__ == "__main__":
    main()
