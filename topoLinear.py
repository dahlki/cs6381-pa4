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

import cs6381_util


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="testing with Mininet Tree Topology")
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")
    parser.add_argument("-p", "--publishers", type=int, default=1, help="number of publishers")
    parser.add_argument("-s", "--subscribers", type=int, default=1, help="number of subscribers")
    parser.add_argument("-r", "--registries", type=int, default=1, help="number of registries")
    parser.add_argument("-n", "--hosts", type=int, default=2, help="number of hosts per switch; default 1")
    parser.add_argument("-k", "--switches", type=int, default=10, help="number of switches; default 10")
    parser.add_argument("-t", "--time", type=int, default=20,
                        help="seconds the program will run before shutting down; default 20")

    return parser.parse_args()


def start_linear_topology(host_num=1, switches=10, strategy="direct", num_pubs=1, num_subs=1, num_registries=1,
                          time_to_run=20):
    hosts = []
    registry_hosts = []
    created_registry_hosts = []
    max_hosts = host_num * switches
    registry_broker = num_registries if strategy == "direct" else (num_registries + 1)
    if max_hosts < (num_pubs + num_subs + registry_broker):
        print("not enough host nodes for number of pubs and subs")
        return

    cleanup()
    net = Mininet(LinearTopo(k=switches, n=host_num))
    net.start()
    time.sleep(1)

    # print(net.hosts)

    for host in net.hosts:
        hosts.append(host)
    # shuffle to get random hosts when running pubs and subs
    random.shuffle(hosts)

    for i in range(num_registries):
        h = hosts.pop()
        host_ip = get_host_ip(h, switches)
        # host_number = h.name[1:]
        # host_ip = "10.0.0.{}".format(host_number)
        print("registry: {}, ip: {}".format(h, host_ip))

        registry_hosts.append({host_ip: h})

    timestamp = cs6381_util.get_timestamp().strftime("%Y-%m-%d %H:%M:%S")

    # run registry
    for count, h in enumerate(registry_hosts):
        def registry_startup_create(registry):
            registry_cmd = "python3 -u cs6381_registry.py -c -p {} -s {} -r {} -d {} -t linear &> 'results/logs/{}-{}-{}-{}-linear-registry-{}-{}.log' &".format(
                num_pubs, num_subs, num_registries, strategy, num_pubs, num_subs, num_registries, strategy, registry.name, timestamp)
            registry.cmd(registry_cmd)

        def registry_startup(registry):
            existing_registry = random.choice(created_registry_hosts)
            registry_cmd = "python3 -u cs6381_registry.py -i {} -p {} -s {} -r {} -d {} -t linear &> 'results/logs/{}-{}-{}-{}-linear-registry-{}-{}.log' &".format(
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
            brokerapp_cmd = "python3 -u brokerapp.py -i {} &> 'results/logs/{}-{}-{}-{}-linear-broker-{}-{}.log' &".format(registry, num_pubs, num_subs, num_registries, strategy,
                                                                                                broker_host.name, timestamp)
            broker_host.cmd(brokerapp_cmd)

        random_registry_host = random.choice(created_registry_hosts)
        broker_startup(random_registry_host)
        time.sleep(1)

    # run pubs
    for _ in itertools.repeat(None, num_pubs):
        def pub_startup(registry):
            pub_host = hosts.pop()
            print("pub: {}, connecting to registry: {}".format(pub_host.name, registry))
            pubapp_cmd = "python3 -u pubapp.py -d {} -i {} &> 'results/logs/{}-{}-{}-{}-linear-pub-{}-{}.log' &".format(strategy, registry, num_pubs, num_subs, num_registries, strategy,
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
            subapp_cmd = "python3 -u subapp.py -d {} -i {} &> 'results/logs/{}-{}-{}-{}-linear-sub-{}-{}.log' &".format(strategy, registry, num_pubs, num_subs, num_registries, strategy,
                                                                                             sub_host.name, timestamp)
            sub_host.cmd(subapp_cmd)

        random_registry_host = random.choice(created_registry_hosts)
        sub_startup(random_registry_host)
        time.sleep(1)

    # give time for subs to get messages
    time.sleep(time_to_run)
    net.stop()
    # dump hosts
    dumpNodeConnections(net.hosts)


def get_host_ip(host, switches):
    print(host)
    # host like h2s6 or h12
    host_number = host.name[1:]
    if "s" in host_number:
        a, b = host_number.split("s")
        a = int(a) - 1
        a = a * switches
        host_num = a + int(b)
        return "10.0.0.{}".format(host_num)
    else:
        return "10.0.0.{}".format(host_number)


def main():
    args = parseCmdLineArgs()

    hosts = args.hosts
    switches = args.switches
    strategy = args.disseminate
    num_pubs = args.publishers
    num_subs = args.subscribers
    time_to_run = args.time
    num_registries = args.registries

    try:
        start_linear_topology(hosts, switches, strategy, num_pubs, num_subs, num_registries, time_to_run)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
