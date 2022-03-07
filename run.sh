#!/bin/bash

DATASET="/home/hsc/dataset/twitter/twitter_rv.net"
# DATASET="/home/hsc/dataset/twitter/w-twitter_rv.net"
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

# ./bin/test/node2vec $DATASET nmblocks $NMBLOCKS sample its weighted length 5 walkpersource 1

# ./bin/test/node2vec $DATASET nmblocks $NMBLOCKS sample opt_alias weighted length 5 walkpersource 2

# ./bin/test/node2vec $DATASET nmblocks $NMBLOCKS sample opt_alias weighted length 25 walkpersource 1

./bin/test/node2vec $DATASET nmblocks $NMBLOCKS sample reject length 2 walkpersource 1

# ./bin/test/autoregressive $DATASET nmblocks $NMBLOCKS sample its length 25 walkpersource 1

# ./bin/test/pagerank $DATASET nmblocks $NMBLOCKS sample naive

# ./bin/test/walk $DATASET nmblocks $NMBLOCKS sample naive walks $STEPS length $LENGTH

# ./bin/test/max_degree $DATASET

# ./bin/test/degree_dist $DATASET $1 $2
