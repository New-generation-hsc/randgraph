#!/bin/bash

DATASET="/home/hsc/dataset/twitter/twitter_rv.net"
VERTICES=61578415
STEPS=100000
LENGTH=25
BLOCKSIZE=65536
NMBLOCKS=16

echo "app = rawrandomwalks, dataset = $DATASET"
echo "vertices = $VERTICES, steps = $STEPS, length = $LENGTH"

sudo sync; sudo sh -c '/usr/bin/echo 1 > /proc/sys/vm/drop_caches'

# the random command
./bin/test/walk $DATASET $1 nmblocks $NMBLOCKS
