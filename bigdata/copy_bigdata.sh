#!/bin/bash

# Bash into cassandra-0 pod
kubectl exec -it cassandra-0 bash

# Create a folder called "bigdata" in the root directory
mkdir /bigdata

# Exit from the pod
exit

# Copy the first .csv file from the local machine to the bigdata folder in cassandra-0 pod
kubectl cp bigdata.csv default/cassandra-0:/bigdata/bigdata.csv