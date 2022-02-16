#!/bin/bash

# DATASET="/home/hsc/dataset/twitter/twitter_rv.net"
DATASET="/home/hsc/dataset/livejournal/soc-LiveJournal1.txt"
# DATASET="/home/hsc/dataset/livejournal/soc-LiveJournal1.txt"
# DATASET="/home/hsc/dataset/livejournal/w-soc-livejournal.txt"
VERTICES=61578415
STEPS=100
# LENGTH=25
LENGTH=1
BLOCKSIZE=65536
NMBLOCKS=6

echo "app = rawrandomwalks, dataset = $DATASET"
echo "vertices = $VERTICES, steps = $STEPS, length = $LENGTH"

sudo sync; sudo sh -c '/usr/bin/echo 1 > /proc/sys/vm/drop_caches'

# the random command
# ./bin/test/node2vec $DATASET $1 nmblocks $NMBLOCKS sample opt_alias walks $STEPS length $LENGTH weighted

./bin/test/pagerank $DATASET nmblocks $NMBLOCKS sample naive

# ./bin/test/walk $DATASET nmblocks $NMBLOCKS sample naive walks $STEPS length $LENGTH

# ./bin/test/max_degree $DATASET
