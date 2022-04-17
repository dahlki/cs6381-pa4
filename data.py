import argparse
import pandas as pd
import os
import csv

import cs6381_util

# for collating total averages from each test/experiment into csv for plotting graphs
class Data:
    def __init__(self, pubs, subs, brokers, regs, topo, dissemination):
        self.pubs = pubs
        self.subs = subs
        self.brokers = brokers
        self.regs = regs
        self.topo = topo
        self.dissemination = dissemination
        self.files = []
        self.nums = [1, 2, 3, 4, 6, 8, 10]
        self.reg_nums = [1, 2, 4, 6, 8, 10]
        self.brokers_nums = [1, 2, 4, 6, 8, 10]
        self.headers = ["pubs/subs", "brokers", "registries", "average", "sub messages"]

    def start(self):
        directory_to_save = ''
        directory_name = '{}-{}-{}-{}-{}'.format(1, 1, 1, self.dissemination, self.topo)
        csv_name = ''
        num_list = self.reg_nums if self.regs else self.nums
        num_list = self.brokers_nums if self.brokers else num_list

        for i in num_list:
            if self.pubs and self.subs:
                directory_to_save = 'pubs-subs-{}-{}'.format(self.dissemination, self.topo)
                directory_name = '{}-{}-{}-{}-{}-{}'.format(i, i, 1, 1, self.dissemination, self.topo)
                csv_name = '{}-{}-{}-{}_{}_{}'.format(i, i, 1, 1, self.dissemination, self.topo)

            elif self.pubs:
                directory_to_save = 'pubs-{}-{}'.format(self.dissemination, self.topo)
                directory_name = '{}-{}-{}-{}-{}-{}'.format(i, 1, 1, 1, self.dissemination, self.topo)
                csv_name = '{}-{}-{}-{}_{}_{}'.format(i, 1, 1, 1, self.dissemination, self.topo)

            elif self.subs:
                directory_to_save = 'subs-{}-{}'.format(self.dissemination, self.topo)
                directory_name = '{}-{}-{}-{}-{}-{}'.format(1, i, 1, 1, self.dissemination, self.topo)
                csv_name = '{}-{}-{}-{}_{}_{}'.format(1, i, 1, 1, self.dissemination, self.topo)

            elif self.regs:
                directory_to_save = 'regs-{}-{}'.format(self.dissemination, self.topo)
                directory_name = '{}-{}-{}-{}-{}-{}'.format(5, 5, 1, i, self.dissemination, self.topo)
                csv_name = '{}-{}-{}-{}_{}_{}'.format(5, 5, 1, i, self.dissemination, self.topo)

            elif self.brokers:
                directory_to_save = 'brokers-{}-{}'.format(self.dissemination, self.topo)
                directory_name = '{}-{}-{}-{}-{}-{}'.format(5, 5, i, 1, self.dissemination, self.topo)
                csv_name = '{}-{}-{}-{}_{}_{}'.format(5, 5, i, 1, self.dissemination, self.topo)

            data_filename = 'results/{}/total_average_{}.csv'.format(directory_name, csv_name)

            data_row = self.get_average_total_data_row(data_filename)
            print("last row", data_row)
            print(f"directory to save: {directory_to_save}")
            # if not os.path.exists('results/{}'.format(directory_to_save)):
            #     os.makedirs('results/{}'.format(directory_to_save))

            file_to_save = 'results/{}/{}.csv'.format(directory_to_save, directory_to_save)
            if cs6381_util.csv_exists('results/{}'.format(directory_to_save)):
                cs6381_util.append_data_row(file_to_save, data_row)
            else:
                os.makedirs('results/{}'.format(directory_to_save))
                with open(file_to_save, "w", encoding="UTF8", newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(self.headers)
                    writer.writerows(data_row)

    def get_average_total_data_row(self, filename):
        print("filename: ", filename)
        if cs6381_util.csv_exists(filename):
            csv_data = pd.read_csv(filename)
            return csv_data.tail(1).values


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="get averages from test runs")
    parser.add_argument("-p", "--publishers", default=False, action="store_true",
                        help="variable publishers")
    parser.add_argument("-s", "--subscribers", default=False, action="store_true",
                        help="variable subscribers")
    parser.add_argument("-b", "--brokers", default=False, action="store_true",
                        help="variable subscribers")
    parser.add_argument("-r", "--registries", default=False, action="store_true",
                        help="variable registries")
    parser.add_argument("-t", "--topo", help="mininet topology; used for data collection info only",
                        choices=["linear", "tree"], type=str)
    parser.add_argument("-d", "--dissemination", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")

    return parser.parse_args()


def main():
    args = parseCmdLineArgs()
    get_data = Data(args.publishers, args.subscribers, args.brokers, args.registries, args.topo, args.dissemination)
    get_data.start()


if __name__ == "__main__":
    main()
