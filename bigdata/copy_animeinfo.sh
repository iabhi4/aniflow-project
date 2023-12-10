#!/bin/bash

# Copy the first .csv file from the local machine to the bigdata folder in cassandra-0 pod
kubectl cp animeinfo.csv default/cassandra-0:/bigdata/animeinfo.csv