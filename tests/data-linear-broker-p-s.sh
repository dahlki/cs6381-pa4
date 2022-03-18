# broker linear p^ s^
sudo python3 topoLinear.py -d broker -p 1 -s 1 -r 1 -t 60
sudo mn -c
sudo python3 topoLinear.py -d broker -p 2 -s 2 -r 1 -t 60
sudo mn -c
sudo python3 topoLinear.py -d broker -p 3 -s 3 -r 1 -t 100
sudo mn -c
sudo python3 topoLinear.py -d broker -p 4 -s 4 -r 1 -t 150
sudo mn -c
sudo python3 topoLinear.py -d broker -p 5 -s 5 -r 1 -t 200
sudo mn -c
sudo python3 topoLinear.py -d broker -p 10 -s 10 -r 1 -t 200 -k 12
sudo mn -c
sudo python3 topoLinear.py -d broker -p 20 -s 20 -r 1 -t 200 -n 3 -k 15
sudo mn -c
