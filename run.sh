#!/bin/bash

set -e

SL_DATASET="/dataset/livejournal/soc-LiveJournal1.txt"
TW_DATASET="/dataset/twitter/twitter_rv.net"
CF_DATASET="/dataset/friendster/friendster.txt"
UK_DATASET="/dataset/uk-union/uk-union.txt"
RM27_DATASET="/dataset/rmat27/rmat27.txt"
RM28_DATASET="/home/hsc/graphdataset/rmat28/rmat28.txt"

DATASET=$SL_DATASET

case $1 in
    "SL") DATASET=$SL_DATASET;;
    "TW") DATASET=$TW_DATASET;;
    "CF") DATASET=$CF_DATASET;;
    "UK") DATASET=$UK_DATASET;;
    "RM27") DATASET=$RM27_DATASET;;
    "RM28") DATASET=$RM28_DATASET;;
    *) DATASET=$SL_DATASET
esac

sudo sync; sudo sh -c '/usr/bin/echo 1 > /proc/sys/vm/drop_caches'

echo "apps = node2vec, dataset = $DATASET, length = 20, sample = reject"

./bin/test/node2vec $DATASET sample reject length 20 walkpersource 1