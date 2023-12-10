#!/bin/bash

# Bash into cassandra-0 pod
kubectl exec -it cassandra-0 bash

# Go to the cqlsh session
cd /opt/cassandra
cqlsh

# Run Cassandra commands
cqlsh -e "create keyspace if not exists anime with replication = {'class':'SimpleStrategy', 'replication_factor':1};"
cqlsh -e "create table anime.animeinfo (anime_id bigint, studio text, genre text, primary key (anime_id));"
cqlsh -e "COPY anime.animeinfo FROM '/bigdata/animeinfo.csv' WITH HEADER = TRUE;"
