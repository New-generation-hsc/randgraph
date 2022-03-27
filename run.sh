#!/bin/bash

set -e

SL_DATASET="/dataset/livejournal/soc-LiveJournal1.txt"
TW_DATASET="/dataset/twitter/twitter_rv.net"
CF_DATASET="/dataset/friendster/friendster.txt"
UK_DATASET="/dataset/uk-union/uk-union.txt"
RM27_DATASET="/dataset/rmat27/rmat27.txt"
RM28_DATASET="/home/hsc/graphdataset/rmat28/rmat28.txt"

# DATASET=$SL_DATASET
# 
# case $1 in
#     "SL") DATASET=$SL_DATASET;;
#     "TW") DATASET=$TW_DATASET;;
#     "CF") DATASET=$CF_DATASET;;
#     "UK") DATASET=$UK_DATASET;;
#     "RM27") DATASET=$RM27_DATASET;;
#     "RM28") DATASET=$RM28_DATASET;;
#     *) DATASET=$SL_DATASET
# esac

# sudo sync; sudo sh -c '/usr/bin/echo 1 > /proc/sys/vm/drop_caches'

# for DATASET in $SL_DATASET $TW_DATASET $CF_DATASET $UK_DATASET $RM27_DATASET $RM28_DATASET
# do
#     echo "apps = autoregressive, dataset = $DATASET, length = 20, sample = reject"
#     ./bin/test/node2vec $DATASET sample reject length 20 walkpersource 1
# done

# for DATASET in $SL_DATASET $TW_DATASET $CF_DATASET $RM27_DATASET $RM28_DATASET
# do
#     echo "apps = autoregressive, dataset = $DATASET, length = 20, sample = reject"
#     ./bin/test/node2vec $DATASET sample reject length 20 walkpersource 1
# done

# for DATASET in $SL_DATASET $TW_DATASET $CF_DATASET $UK_DATASET $RM27_DATASET $RM28_DATASET
# do
#     echo "apps = autoregressive, dataset = $DATASET, length = 20, sample = reject"
#     ./bin/test/autoregressive $DATASET sample its length 20 walksource 1000000
# done

# for CACHE in 1 2 4 6 8 10
# do
#     echo "apps = node2vec, dataset = $CF_DATASET, length = 20, sample = reject, cache_size = $CACHE"
#     ./bin/test/node2vec $CF_DATASET sample reject length 20 walkpersource 1 cache $CACHE
# done

# for DATASET in $TW_DATASET $CF_DATASET $RM28_DATASET
# do
#     for ITER in 100 200 300 400 500 600 700 800 900 1000
#     do
#         echo "apps = node2vec, dataset = $DATASET, length = 20, sample = reject"
#         ./bin/test/node2vec $DATASET sample reject length 20 walkpersource 1 iter $ITER
#     done
# done

for ITER in 400 800 900 1000
do
    echo "apps = node2vec, dataset = $UK_DATASET, length = 20, sample = reject"
    ./bin/test/node2vec $UK_DATASET sample reject length 20 walkpersource 1 iter $ITER
done
