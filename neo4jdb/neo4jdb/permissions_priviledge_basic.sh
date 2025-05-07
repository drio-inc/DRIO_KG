#!/bin/bash

echo "initial.dbms.default_database=mydatabase" >> /var/lib/neo4j/conf/neo4j.conf
echo "server.bolt.tls_level=DISABLED" >> /var/lib/neo4j/conf/neo4j.conf