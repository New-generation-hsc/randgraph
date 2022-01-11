#!/bin/bash

# DATASET="/home/hsc/dataset/twitter/twitter_rv.net"
DATASET="/home/hsc/dataset/livejournal/w-soc-livejournal.txt"
VERTICES=61578415
STEPS=100
# LENGTH=25
LENGTH=1
BLOCKSIZE=65536
NMBLOCKS=16

echo "app = rawrandomwalks, dataset = $DATASET"
echo "vertices = $VERTICES, steps = $STEPS, length = $LENGTH"

sudo sync; sudo sh -c '/usr/bin/echo 1 > /proc/sys/vm/drop_caches'

# the random command
./bin/test/node2vec $DATASET $1 nmblocks $NMBLOCKS sample its walks $STEPS length $LENGTH weighted
