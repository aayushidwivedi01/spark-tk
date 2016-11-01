import random
from numpy import array
from collections import Counter

gender = ['F', 'M']
relationship = ['follower', 'friend']

data = [line.strip("\n").split(",") for line in open("../datasets/clique_10.csv").readlines()]
verticies = set([value for edge in data for value in edge])
label = {vertex:random.choice(gender) for vertex in verticies}

with open("clique_with_vertex_type.csv", "w") as f:
    for src, dst in data:
        f.write(src + "," + dst + "," + label[src] + "," + label[dst] + "," + random.choice(relationship) + "\n")

