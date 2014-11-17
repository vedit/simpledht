#!/bin/bash

ant -buildfile simpledht.xml clean all build-jar
ant -buildfile simpledht.xml run1 &
ant -buildfile simpledht.xml run2 &