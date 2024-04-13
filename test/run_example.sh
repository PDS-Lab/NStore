#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/

loadsize=10000000 # alexol needs to bulkload 10M sorted kvs
putsize=10000000
getsize=10000000
threadnum=16 # thread number per socket

function Run()
{
  loadsize=$1
  putsize=$2
  getsize=$3
  threadnum=$4

  rm -rf /mnt/pmem0/lbl/*
  rm -rf /mnt/pmem1/lbl/*
  date | tee example_output.txt
  timeout 1200 ${BUILDDIR}example  --load-size ${loadsize} --put-size ${putsize} --get-size ${getsize}  -t ${threadnum} | tee -a example_output.txt
}

Run $dbname $loadsize $putsize $getsize $threadnum