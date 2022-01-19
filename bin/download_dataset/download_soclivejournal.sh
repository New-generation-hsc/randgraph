#!/bin/bash

# exit immediately if a command exists with non-zero exports
set -e

# the download dataset name
dataset=soc-livejournal.txt

# the dataset download directory
data_dir=$1

if [[ -z ${data_dir}  ]]
then
    data_dir=$(pwd)
fi

mkdir -p ${data_dir}

dataset_path=${data_dir}/${dataset}

echo "the soc-livejournal dataset output path : ${dataset_path}"

if [[ ! -f ${dataset_path} ]]
then
    echo "start to download soc-livejournal.txt dataset, please wait..."
    wget https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz -O ${dataset_path}.gz
    echo "unzip the soc-livejournal.txt.gz..."
    gunzip -c ${dataset_path}.gz > ${dataset_path}
    echo "delete the download temporary file..."
    rm -f ${dataset_path}.gz
else
    echo "the soc-livejournal.txt dataset already exists, skip the download process"
fi

echo "successfully down!"
