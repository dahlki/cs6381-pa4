# script for generating graphs using plots.py

python3 plots.py -f results/pubs-broker-linear/pubs-broker-linear.csv -g pubs-broker-linear -t 'increasing pubs - one registry - broker dissemination - linear topology'
python3 plots.py -f results/pubs-broker-tree/pubs-broker-tree.csv -g pubs-broker-tree -t 'increasing pubs - one registry - broker dissemination - tree topology'
python3 plots.py -f results/pubs-direct-linear/pubs-direct-linear.csv -g pubs-direct-linear -t 'increasing pubs - one registry - direct dissemination - linear topology'
python3 plots.py -f results/pubs-direct-tree/pubs-direct-tree.csv -g pubs-direct-tree -t 'increasing pubs - one registry - direct dissemination - tree topology' -u .001

python3 plots.py -f results/pubs-subs-broker-linear/pubs-subs-broker-linear.csv -g pubs-subs-broker-linear -t 'increasing pubs and subs - one registry - broker dissemination - linear topology'
python3 plots.py -f results/pubs-subs-broker-tree/pubs-subs-broker-tree.csv -g pubs-subs-broker-tree -t 'increasing pubs and subs - one registry - broker dissemination - tree topology'
python3 plots.py -f results/pubs-subs-direct-linear/pubs-subs-direct-linear.csv -g pubs-subs-direct-linear -t 'increasing pubs -and subs  one registry - direct dissemination - linear topology'
python3 plots.py -f results/pubs-subs-direct-tree/pubs-subs-direct-tree.csv -g pubs-subs-direct-tree -t 'increasing pubs and subs - one registry - direct dissemination - tree topology' -u .001

python3 plots.py -f results/regs-broker-linear/regs-broker-linear.csv -g regs-broker-linear -t 'increasing regs - 5 pubs - 5 subs - broker dissemination - linear topology'
python3 plots.py -f results/regs-broker-tree/regs-broker-tree.csv -g regs-broker-tree -t 'increasing regs - 5 pubs - 5 subs - broker dissemination - tree topology' -u .001
python3 plots.py -f results/regs-direct-linear/regs-direct-linear.csv -g regs-direct-linear -t 'increasing regs - 5 pubs - 5 subs - direct dissemination - linear topology' -u .001
python3 plots.py -f results/regs-direct-tree/regs-direct-tree.csv -g regs-direct-tree -t 'increasing regs - 5 pubs - 5 subs - direct dissemination - tree topology' -u .001

python3 plots.py -f results/subs-broker-linear/subs-broker-linear.csv -g subs-broker-linear -t 'increasing subs - one registry - broker dissemination - linear topology'
python3 plots.py -f results/subs-broker-tree/subs-broker-tree.csv -g subs-broker-tree -t 'increasing subs - one registry - broker dissemination - tree topology'
python3 plots.py -f results/subs-direct-linear/subs-direct-linear.csv -g subs-direct-linear -t 'increasing subs - one registry - direct dissemination - linear topology'
python3 plots.py -f results/subs-direct-tree/subs-direct-tree.csv -g subs-direct-tree -t 'increasing subs - one registry - direct dissemination - tree topology' -u .001
