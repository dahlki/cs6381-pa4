import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import csv
import argparse


# cwd = os.getcwd()  # Get the current working directory (cwd)
# files = os.listdir(cwd)  # Get all the files in that directory
# print("Files in %r: %s" % (cwd, files))


class Plot:
    def __init__(self, csv_filepath, graph_filename):
        self.x = []
        self.y = []
        self.csv_filepath = csv_filepath
        self.graph_filename = graph_filename

    def plot(self):

        csv_data = pd.read_csv(self.csv_filepath)
        df = pd.DataFrame(csv_data)
        df.sort_values(by="pubs/subs")
        # sorted_data = csv_data.sort_values(['pubs/subs'], inplace=True)
        print(df)

        with open(self.csv_filepath, 'r') as csv_file:
            lines = csv.reader(csv_file, delimiter=',')
            first_line = csv_file.readline()
            # lines = sorted(lines, key=lambda col: (col[0], col[1]))

            for row in lines:
                self.x.append(row[0])
                self.y.append(float(row[1]))

        f = plt.figure()
        f.set_figwidth(10)
        f.set_figheight(5)
        plt.plot(self.x, self.y, color='g', linestyle='dashed', marker='o', label="average time delay")

        plt.yticks(np.arange(min(self.y), max(self.y), .1))
        plt.xticks(rotation=25)
        plt.xlabel('pubs/subs')
        plt.ylabel('delay in seconds')
        plt.title('one sub, increasing number of pubs - broker dissemination', fontsize=20)
        plt.grid()
        plt.legend()
        plt.tight_layout()

        for i, label in enumerate(self.y):
            plt.text(self.x[i], self.y[i], self.y[i])

        plt.savefig(self.graph_filename)
        plt.show()


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Plotting Application")
    parser.add_argument("-f", "--file", type=str, required=True, help="path to csv file")
    parser.add_argument("-g", "--graph", type=str, required=True, help="name of png file to save graph")

    return parser.parse_args()


def main():
    args = parseCmdLineArgs()
    plot = Plot(args.file, args.graph)
    plot.plot()


if __name__ == "__main__":
    main()
