#!/bin/bash
cleanup() {
  ps uax | grep com.isikun.firat.dht.simplified | awk '{ print $2 }' | xargs kill
}
cleanup
