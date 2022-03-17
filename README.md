# cs6381: distributed systems principles

### programming assignment 2: a Kademlia DHT-based discovery, multi-dissemination strategy publish-subscribe middleware application utilizing ZeroMQ
<br/>

<br/>


**testing:** run in Ubuntu virtual machine using Mininet

**suggestion:** run sudo mn -c before each test run

**below approaches are not in any particular order**

<br/>

## approach one - using xterms:

create mininet topology, for example:
~~~
sudo mn --topo=tree,depth=3,fanout=3
sudo mn --topo=linear,10
~~~
in any xterm window, create registry server (use -c option for first registry):
~~~
sudo python3 registry.py -d {direct, broker} -p (number of publishers) -s (number of subscrbers) -r (number of registries) -c (if first registry) -t {tree, linear}
~~~
~~~
  -h, --help            show this help message and exit
  -d {direct,broker}, --disseminate {direct,broker}
                        Dissemination strategy: direct or via broker; default is direct
  -p PUBLISHERS, --publishers PUBLISHERS
                        number of publishers that need to register before dissemination begins
  -s SUBSCRIBERS, --subscribers SUBSCRIBERS
                        number of subscribers that need to register before dissemination begins
  -r REGISTRIES, --registries REGISTRIES
                        number of registries; used for data collection info only
  -c, --create          Create a new DHT ring, otherwise we join a DHT
  -l, --debug           Logging level (see logging package): default WARNING
  -i IPADDR, --ipaddr IPADDR
                        IP address of any existing DHT node
  -o PORT, --port PORT  port number used by one or more DHT nodes
  -t {linear,tree}, --topo {linear,tree} 
                        mininet topology
~~~
- the purpose for -r and -t are solely for filenames of logging output
<br/><br/>

if using the broker dissemination strategy (can be opened in any host):
~~~
sudo python3 brokerapp.py -p (port number)
~~~
- skip the above if using direct dissemination
<br/><br/>

publishers:
~~~
sudo python3 pubapp.py -d {direct, broker} -p (port number) -n (number of topics) -i (ip address of existing dht registry) -t (time delay between messages published)
~~~

subscribers:
~~~
sudo pythons subapp.py -d {direct, broker} -p (port number) -n (number of topics) -i (ip address of existing dht registry) -r (number of data rows to be collected)
~~~
cmd line argument options for running _pubapp.py and subapp.py_:
~~~
  -h, --help            show this help message and exit
  -d {direct,broker}, --disseminate {direct,broker}
                        Dissemination strategy: direct or via broker; default is direct
  -p PORT, --port PORT  specify port number; default is 5555 for pubs and 5556 for subs
  -n {1,2,3,4,5,6,7,8}, --number {1,2,3,4,5,6,7,8}
                        number of topics to publish; between 1 and 8; default will be random number of topics
~~~
cmd line argument option for _pubapp.py_:
~~~
  -t {0,1,2,3}, --time {0,1,2,3}
                        specify seconds between publishing messages; default is 0
~~~

cmd line argument option for _subapp.py_:
~~~
    -r ROWS, --rows ROWS  total number of data rows to be collected before termination; default is 50

~~~

example:
~~~
sudo python3 cs6381_registry.py -c -d broker -p 3 -s 2 -r 2
sudo python3 cs6381_registry.py -d broker -i (ip address of first registry created)
sudo python3 brokerapp.py
sudo python3 pubapp.py -d broker -p 5050 -n 2
sudo python3 pubapp.py -d broker -p 5050 -n 10
sudo python3 pubapp.py -d broker -p 5050 -n 1
sudo python3 subapp.py -d brpker -p 5055 -r 100 -n 5
sudo python3 subapp.py -d brpker -p 5052 -r 25 -n 2
~~~
- logs will be created in the results directory
<br/>

<br/>

## approach two - use topoTree.py or topoLinear.py to create a tree or linear network and run the application:


in the ubuntu vm terminal for Tree topology:
~~~
sudo python3 topoTree.py -d {direct, broker} -p (number of publishers) -s (number of subscribers) -r (number of registries) -l (tree depth) -f (fanout number) -t (number of seconds to let the program run)
~~~
~~~
  -h, --help            show this help message and exit
  -d {direct,broker}, --disseminate {direct,broker}
                        Dissemination strategy: direct or via broker; default is direct
  -p PUBLISHERS, --publishers PUBLISHERS
                        number of publishers
  -s SUBSCRIBERS, --subscribers SUBSCRIBERS
                        number of subscribers
  -r REGISTRIES, --registries REGISTRIES
                        number of registries
  -l DEPTH, --depth DEPTH
                        depth of tree topo; default 3
  -f FANOUT, --fanout FANOUT
                        fanout value of tree topo; default 3
  -t TIME, --time TIME  seconds the program will run before shutting down; default 20

~~~
example with direct dissemination and 2 pubs, 4 subs, and 2 registries:
~~~
sudo python3 topoTree.py -p 2 -s 4 -r 2
~~~
- default is direct dissemination and mininet topology tree of depth 3 with fanout 3

example with broker dissemination and 3 pubs, 3 subs, and 4 registries:
~~~
sudo python3 topoTree.py -d broker -p 3 -s 3 -r 4 -l 2 -f 4 -t 25
~~~
- program will run for about 20 seconds to allow enough time for logs to be generated, which can be viewed in the results directory
- increase the time in seconds, if running many pubs and subs 
<br/>
<br/>

in the ubuntu vm terminal for Linear topology:
~~~
sudo python3 topoLinear.py -d {direct, broker} -p (number of publishers) -s (number of subscribers) -r (number of registries) -n (number of hosts per switch) -k (number of switches) -t (number of seconds to let the program run)
~~~
~~~
  -h, --help            show this help message and exit
  -d {direct,broker}, --disseminate {direct,broker}
                        Dissemination strategy: direct or via broker; default is direct
  -p PUBLISHERS, --publishers PUBLISHERS
                        number of publishers
  -s SUBSCRIBERS, --subscribers SUBSCRIBERS
                        number of subscribers
  -r REGISTRIES, --registries REGISTRIES
                        number of registries
  -n HOSTS, --hosts HOSTS
                        number of hosts per switch; default 1
  -k SWITCHES, --switches SWITCHES
                        number of switches; default 20

  -t TIME, --time TIME  seconds the program will run before shutting down; default 20
  
~~~
example with direct dissemination and 3 pubs, 4 subs, and 3 registries:
~~~
sudo python3 topoTree.py -p 3 -s 5 -r 3 -n 2
~~~
- default is direct dissemination and mininet linear topology with 10 switches and 1 host per switch

example with broker dissemination and 2 pubs, 6 subs, 2 registries, 6 switches with 2 hosts each:
~~~
sudo python3 topoTree.py -d broker -p 3 -s 3 -r 2 -n 2 -k 6 -t 25
~~~

<br/>

## approach three - use text files:


create a text file with commands to run the applications

example with broker dissemination and 2 pub (publishing 8 topics each) and 5 subs:
~~~
h1 python3 -u cs6381_registry.py -c -p 1 -s 5 -r 2 -t tree -d broker &> results/1-5-2-broker-tree-logs/registry1.out &
h3 python3 -u cs6381_registry.py -i 10.0.0.1 -p 1 -s 5 -t tree -d broker &> results/1-5-2-broker-tree-logs/registry2.out &

h2 python3 -u brokerapp.py -i 10.0.0.1 &> results/1-5-2-broker-tree-logs/broker.out &

h5 python3 -u pubapp.py -i 10.0.0.1 -d broker -n 8 &> results/1-5-2-broker-tree-logs/publisher1.out &
h12 python3 -u pubapp.py -i 10.0.0.1 -d broker -n 8 &> results/1-5-2-broker-tree-logs/publisher2.out &

h8 python3 -u subapp.py -i 10.0.0.1 -d broker &> results/1-5-2-broker-tree-logs/subscriber1.out &
h15 python3 -u subapp.py -i 10.0.0.1 -d broker &> results/1-5-2-broker-tree-logs/subscriber2.out &
h4 python3 -u subapp.py -i 10.0.0.1 -d broker &> results/1-5-2-broker-tree-logs/subscriber3.out &
h9 python3 -u subapp.py -i 10.0.0.1 -d broker &> results/1-5-2-broker-tree-logs/subscriber4.out &
h6 python3 -u subapp.py -i 10.0.0.1 -d broker &> results/1-5-2-broker-tree-logs-logs/subscriber5.out &
~~~

in the ubuntu vm terminal, create a mininet topology, making sure there are enough hosts to accommodate the registry + broker + pubs + subs
~~~
sudo mn --topo=tree,depth=3,fanout=3
~~~
also, be aware of which hostnames you use in reference to how many you create 

run the file in mininet with:
~~~
source <path_to_file/filename>
~~~
