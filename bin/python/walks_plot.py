import matplotlib.pyplot as plt
import numpy as np
import argparse

parser = argparse.ArgumentParser(description='Process drunkardmob log file')
parser.add_argument("--file", type=str, help="the drunkardmob log file path", default="/home/hsc/randgraph/dist.log")
parser.add_argument("--step", type=int, help="the step", default=3)

args = parser.parse_args()
walks = []
with open(args.file, "r", encoding="utf-8") as file:
    for line in file:
        walks.append(list(map(int, line.split())))

x = np.arange(len(walks[0]))
y = np.array(walks[args.step])

plt.stem(x, y)
plt.show()