#!/bin/bash
kill_processes() {
  ps uax | grep com.isikun.firat.dht.simplified | awk '{ print $2 }' | xargs kill
}

reset_folders() {
  mkdir -p data$1
  rm data$1/*
}
kill_processes
for i in {1..4}
do
  reset_folders $i
done

cp data/* data1
