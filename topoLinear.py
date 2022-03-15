# import sys
# print(sys.path)
import random

from mininet.net import Mininet
from mininet.topolib import TreeNet
from mininet.topo import Topo
from mininet.topo import LinearTopo
from mininet.util import dumpNodeConnections
import argparse
from mininet.clean import cleanup
import itertools
import time


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="testing with Mininet Tree Topology")
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")
    parser.add_argument("-p", "--publishers", type=int, default=1, help="number of publishers")
    parser.add_argument("-s", "--subscribers", type=int, default=1, help="number of subscribers")
    parser.add_argument("-n", "--hosts", type=int, default=1, help="number of hosts per switch; default 1")
    parser.add_argument("-k", "--switches", type=int, default=10, help="number of switches; default 10")
    parser.add_argument("-t", "--time", type=int, default=10, help="seconds the program will run before shutting down; default 10")

    return parser.parse_args()


def start_linear_topology(host_num=1, switches=10, strategy="direct", num_pubs=1, num_subs=1, time_to_run=30):
    hosts = []
    threads = []
    max_hosts = host_num * switches
    registry_broker = 1 if strategy == "direct" else 2

    if max_hosts < (num_pubs + num_subs + registry_broker):
        print("not enough host nodes for number of pubs and subs")

    cleanup()
    net = Mininet(LinearTopo(k=switches, n=host_num))
    net.start()

    print(net.hosts)

    for host in net.hosts[1:]:
        hosts.append(host)
    # shuffle to get random hosts when running pubs and subs
    random.shuffle(hosts)

    # run registry
    def registry_startup():
        registry_host = net.hosts[0]
        registry_cmd = "python3 -u cs6381_registry.py -p {} -s {} -d {} -t linear &> 'results/registry-{}.log' &".format(num_pubs, num_subs, strategy, registry_host.name)
        registry_host.cmd(registry_cmd)
    registry_startup()

    # run broker if dissemination strategy is broker
    if strategy == "broker":
        def broker_startup():
            broker_host = hosts.pop()
            brokerapp_cmd = "python3 -u brokerapp.py -i {} &> 'results/broker-{}.log' &".format("10.0.0.1", broker_host.name)
            broker_host.cmd(brokerapp_cmd)
        broker_startup()

    # run pubs
    for _ in itertools.repeat(None, num_pubs):
        def pub_startup():
            pub_host = hosts.pop()
            pubapp_cmd = "python3 -u pubapp.py -d {} -i {} &> 'results/pub-{}.log' &".format(strategy, "10.0.0.1", pub_host.name)
            pub_host.cmd(pubapp_cmd)
        pub_startup()

    # run subs
    for i in range(num_subs):
        def sub_startup():
            sub_host = hosts.pop()
            subapp_cmd = "python3 -u subapp.py -d {} -i {} &> 'results/sub-{}.log' &".format(strategy, "10.0.0.1", sub_host.name)
            sub_host.cmd(subapp_cmd)
        sub_startup()

    # give time for subs to get messages
    time.sleep(time_to_run)
    net.stop()
    # dump hosts
    dumpNodeConnections(net.hosts)


def main():
    args = parseCmdLineArgs()

    hosts = args.hosts
    switches = args.switches
    strategy = args.disseminate
    num_pubs = args.publishers
    num_subs = args.subscribers
    time_to_run = args.time

    start_linear_topology(hosts, switches, strategy, num_pubs, num_subs, time_to_run)


if __name__ == "__main__":
    main()