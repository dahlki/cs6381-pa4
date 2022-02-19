# cs6381: distributed systems principles

### programming assignment 1: a centralized discovery, multi-dissemination strategy publish-subscribe middleware application utilizing ZeroMQ
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
~~~
in xterm h1 window (registry must be at host h1 / ip 10.0.0.1):
~~~
sudo python3 registry.py -d {direct, broker} -p (number of publishers) -s (number of subscrbers) 
~~~
~~~
  -h, --help            show this help message and exit
  -d {direct,broker}, --disseminate {direct,broker}
                        Dissemination strategy: direct or via broker; default is direct
  -p PUBLISHERS, --publishers PUBLISHERS
                        number of publishers
  -s SUBSCRIBERS, --subscribers SUBSCRIBERS
                        number of subscribers

~~~
- for now -p and -s are optional (used just for filenames of logging output)
<br/><br/>

if using the broker dissemination strategy (can be opened in any host):
~~~
sudo python3 brokerapp.py -p (port number)
~~~
- skip the above if using direct dissemination
<br/><br/>

publishers:
~~~
sudo python3 pubapp.py -d {direct, broker} -p (port number) -n (number of topics)
~~~

subscribers:
~~~
sudo pythons subapp.py -d {direct, broker} -p (port number) -n (number of topics)
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

example:
~~~
sudo python3 cs6381_registry.py -d broker -p 1 -s 2
sudo python3 brokerapp.py
sudo python3 pubapp.py -d broker -p 5050 -n 2
sudo python3 subapp.py -d brpker -p 5055
sudo python3 subapp.py -d brpker -p 5052
~~~
- logs will be created in the results directory
<br/>

<br/>

## approach two - use topo.py to create a tree network and run the application:


in the ubuntu vm terminal:
~~~
sudo python3 topo.py -d {direct, broker} -p (number of publishers) -s (number of subscribers) -l (tree depth) -f (fanout number)
~~~
~~~
  -h, --help            show this help message and exit
  -d {direct,broker}, --disseminate {direct,broker}
                        Dissemination strategy: direct or via broker; default is direct
  -p PUBLISHERS, --publishers PUBLISHERS
                        number of publishers
  -s SUBSCRIBERS, --subscribers SUBSCRIBERS
                        number of subscribers
  -l DEPTH, --depth DEPTH
                        depth of tree topo; default 3
  -f FANOUT, --fanout FANOUT
                        fanout value of tree topo; default 3
  -t TIME, --time TIME  seconds the program will run before shutting down; default 10

~~~
example with direct dissemination and 2 pubs and 4 subs:
~~~
sudo python3 topo.py -p 2 -s 4
~~~
- default is direct dissemination and mininet topology tree of depth 3 with fanout 3

example with broker dissemination and 1 pubs and 4 subs:
~~~
sudo python3 topo.py -d broker -p 3 -s 3 -l 2 -f 4 -t 15
~~~
- program will run for about 10 seconds to allow enough time for logs to be generated, which can be viewed in the results directory
- increase the time in seconds, if running many pubs and subs 
<br/>

<br/>

## approach three - use text files:


create a text file with commands to run the applications

example with broker dissemination and 2 pub (publishing 8 topics each) and 5 subs:
~~~
h1 python3 -u cs6381_registry.py -p 1 -s 5 -d broker &> results/1-5-broker/registry.out &
h2 python3 -u brokerapp.py &> results/1-5-broker/broker.out &

h5 python3 -u pubapp.py -d broker -n 8 &> results/1-5-broker/publisher1.out &
h12 python3 -u pubapp.py -d broker -n 8 &> results/1-5-broker/publisher2.out &

h8 python3 -u subapp.py -d broker &> results/1-5-broker/subscriber1.out &
h15 python3 -u subapp.py -d broker &> results/1-5-broker/subscriber2.out &
h4 python3 -u subapp.py -d broker &> results/1-5-broker/subscriber3.out &
h9 python3 -u subapp.py -d broker &> results/1-5-broker/subscriber4.out &
h6 python3 -u subapp.py -d broker &> results/1-5-broker/subscriber5.out &
~~~

in the ubuntu vm terminal, create a mininet topology, making sure there are enough hosts to accommodate the registry + broker + pubs + subs

also, be aware of which hostnames you use in reference to how many you create 

run the file in mininet with:
~~~
source <path_to_file/filename>
~~~
