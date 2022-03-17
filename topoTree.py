# import sys
# print(sys.path)
import random

from mininet.net import Mininet
from mininet.topolib import TreeNet
from mininet.topo import Topo
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
    parser.add_argument("-r", "--registries", type=int, default=1, help="number of registries")
    parser.add_argument("-l", "--depth", type=int, default=3, help="depth of tree topo; default 3")
    parser.add_argument("-f", "--fanout", type=int, default=3, help="fanout value of tree topo; default 3")
    parser.add_argument("-t", "--time", type=int, default=20, help="seconds the program will run before shutting down; default 20")

    return parser.parse_args()


def start_tree_topology(depth=3, fanout=3, strategy="direct", num_pubs=1, num_subs=1, num_registries=1, time_to_run=10):
    hosts = []
    registry_hosts = []
    created_registry_hosts = []
    max_hosts = fanout ** depth
    registry_broker = num_registries if strategy == "direct" else (num_registries + 1)
    if max_hosts < (num_pubs + num_subs + registry_broker):
        print("not enough host nodes for number of pubs and subs")

    cleanup()
    net = TreeNet(depth=depth, fanout=fanout)
    # net = Mininet(topo=tree)
    net.start()

    # print(net.hosts)

    for host in net.hosts[1:]:
        hosts.append(host)
    # shuffle to get random hosts when running pubs and subs
    random.shuffle(hosts)

    for i in range(num_registries):
        h = hosts.pop()
        host_number = h.name[1:]
        host_ip = "10.0.0.{}".format(host_number)
        print("registry: {}, ip: {}".format(h, host_ip))

        registry_hosts.append({host_ip: h})

    timestamp = cs6381_util.get_timestamp().strftime("%Y-%m-%d %H:%M:%S")

    # run registry
    for count, h in enumerate(registry_hosts):
        def registry_startup_create(registry):
            registry_cmd = "python3 -u cs6381_registry.py -c -p {} -s {} -r {} -d {} -t tree &> 'results/{}-{}-{}-{}-tree-registry-{}-{}.log' &".format(
                num_pubs, num_subs, num_registries, strategy, num_pubs, num_subs, num_registries, strategy, registry.name, timestamp)
            registry.cmd(registry_cmd)

        def registry_startup(registry):
            existing_registry = random.choice(created_registry_hosts)
            registry_cmd = "python3 -u cs6381_registry.py -i {} -p {} -s {} -r {} -d {} -t tree &> 'results/{}-{}-{}-{}-tree-registry-{}-{}.log' &".format(
                existing_registry, num_pubs, num_subs, num_registries, strategy, num_pubs, num_subs, num_registries, strategy, registry.name, timestamp)
            registry.cmd(registry_cmd)

        [[host_ip, registry_host]] = h.items()
        if count == 0:
            registry_startup_create(registry_host)
        else:
            registry_startup(registry_host)
        created_registry_hosts.append(host_ip)
        time.sleep(1)

    # run broker if dissemination strategy is broker
    if strategy == "broker":
        def broker_startup(registry):
            broker_host = hosts.pop()
            print("broker: {}, connecting to registry: {}".format(broker_host.name, registry))
            brokerapp_cmd = "python3 -u brokerapp.py -i {} &> 'results/{}-{}-{}-{}-tree-broker-{}-{}.log' &".format(registry, num_pubs, num_subs, num_registries, strategy, broker_host.name, timestamp)
            broker_host.cmd(brokerapp_cmd)

        random_registry_host = random.choice(created_registry_hosts)
        broker_startup(random_registry_host)
        time.sleep(1)

    # run pubs
    for _ in itertools.repeat(None, num_pubs):
        def pub_startup(registry):
            pub_host = hosts.pop()
            print("pub: {}, connecting to registry: {}".format(pub_host.name, registry))
            pubapp_cmd = "python3 -u pubapp.py -d {} -i {} &> 'results/{}-{}-{}-{}-tree-pub-{}-{}.log' &".format(strategy, registry, num_pubs, num_subs, num_registries, strategy,
                                                                                             pub_host.name, timestamp)
            pub_host.cmd(pubapp_cmd)

        random_registry_host = random.choice(created_registry_hosts)
        pub_startup(random_registry_host)
        time.sleep(1)

        # run subs
    for i in range(num_subs):
        def sub_startup(registry):
            sub_host = hosts.pop()
            print("sub: {}, connecting to registry: {}".format(sub_host.name, registry))
            subapp_cmd = "python3 -u subapp.py -d {} -i {} &> 'results/{}-{}-{}-{}-tree-sub-{}-{}.log' &".format(strategy, registry, num_pubs, num_subs, num_registries, strategy,
                                                                                             sub_host.name)
            sub_host.cmd(subapp_cmd)

        random_registry_host = random.choice(created_registry_hosts)
        sub_startup(random_registry_host)
        time.sleep(1)

    # give time for subs to get messages
    time.sleep(time_to_run)
    net.stop()
    # dump hosts
    dumpNodeConnections(net.hosts)


def main():
    args = parseCmdLineArgs()

    depth = args.depth
    fanout = args.fanout
    strategy = args.disseminate
    num_pubs = args.publishers
    num_subs = args.subscribers
    time_to_run = args.time
    num_registries = args.registries

    start_tree_topology(depth, fanout, strategy, num_pubs, num_subs, num_registries, time_to_run)


if __name__ == "__main__":
    main()