# broker tree p- s^
sudo python3 topoTree.py -d broker -p 1 -s 1 -r 1 -t 60
sudo mn -c
sudo python3 topoTree.py -d broker -p 1 -s 2 -r 1 -t 60
sudo mn -c
sudo python3 topoTree.py -d broker -p 1 -s 3 -r 1 -t 100
sudo mn -c
sudo python3 topoTree.py -d broker -p 1 -s 4 -r 1 -t 120
sudo mn -c
sudo python3 topoTree.py -d broker -p 1 -s 5 -r 1 -t 120
sudo mn -c
sudo python3 topoTree.py -d broker -p 1 -s 10 -r 1 -t 120
sudo mn -c
sudo python3 topoTree.py -d broker -p 1 -s 20 -r 1 -t 200 -l 4
sudo mn -c
