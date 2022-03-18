import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import csv
import argparse


# cwd = os.getcwd()  # Get the current working directory (cwd)
# files = os.listdir(cwd)  # Get all the files in that directory
# print("Files in %r: %s" % (cwd, files))


class Plot:
    def __init__(self, csv_filepath, graph_filename, title, ticks):
        self.x = []
        self.y = []
        self.csv_filepath = csv_filepath
        self.graph_filename = graph_filename
        self.title = title
        self.ticks = ticks

    def plot(self):

        is_registry_plot = "regs" in self.csv_filepath

        csv_data = pd.read_csv(self.csv_filepath)
        df = pd.DataFrame(csv_data)
        # if not is_registry_plot:
        #     df.sort_values(by="pubs/subs")
        # else:
        #     df.sort_values(by="registries")
        # sorted_data = csv_data.sort_values(['pubs/subs'], inplace=True)
        print(df)

        with open(self.csv_filepath, 'r') as csv_file:
            lines = csv.reader(csv_file, delimiter=',')
            first_line = csv_file.readline()
            # lines = sorted(lines, key=lambda col: (col[0], col[1]))

            for row in lines:
                print(row)
                if not is_registry_plot:
                    self.x.append(row[0])
                    x = [1, 2, 3, 4, 5, 10, 20]
                else:
                    self.x.append(row[1])
                    x = [1, 2, 4, 6, 8, 10]
                self.y.append(float(row[2]))
        print(self.x)
        print(self.y)

        f = plt.figure()
        f.set_figwidth(10)
        f.set_figheight(5)


        plt.plot(x, self.y, color='g', linestyle='dashed', marker='o', label="average time delay")

        plt.yticks(np.arange(min(self.y), max(self.y), self.ticks))
        plt.xticks(x, self.x, rotation=25)
        if not is_registry_plot:
            plt.xlabel('pubs/subs')
        else:
            plt.xlabel('registries')
        plt.ylabel('delay in seconds')
        plt.title(self.title, fontsize=20)
        plt.grid()
        plt.legend()
        plt.tight_layout()

        for i, label in enumerate(self.y):
            plt.text(x[i], self.y[i], self.y[i])

        plt.savefig('results/{}/{}'.format(self.graph_filename, self.graph_filename))
        plt.show()


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Plotting Application")
    parser.add_argument("-f", "--file", type=str, required=True, help="path to csv file")
    parser.add_argument("-g", "--graph", type=str, required=True, help="name of png file to save graph")
    parser.add_argument("-t", "--title", type=str, required=True, help="title of graph")
    parser.add_argument("-u", "--ticks", type=float, default=.0001,
                        help="y axis representation of units in ms; default .0001")

    return parser.parse_args()


def main():
    args = parseCmdLineArgs()
    plot = Plot(args.file, args.graph, args.title, args.ticks)
    plot.plot()


if __name__ == "__main__":
    main()
