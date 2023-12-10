#!/bin/bash

# Bash into cassandra-0 pod
kubectl exec -it cassandra-0 bash

# Go to the cqlsh session
cd /opt/cassandra
cqlsh

# Run Cassandra commands
cqlsh -e "create keyspace if not exists anime with replication = {'class':'SimpleStrategy', 'replication_factor':1};"
cqlsh -e "create table anime.animedata (id bigint, anime_id bigint, my_score bigint, my_status bigint, user_id bigint, gender text, age bigint, primary key (id));"
cqlsh -e "COPY anime.animedata FROM '/bigdata/bigdata.csv' WITH HEADER = TRUE;"