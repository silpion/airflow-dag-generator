#!/bin/sh
docker compose up -d
sleep 10
docker exec -it dwh_pg psql -d dwh -c 'IMPORT FOREIGN SCHEMA public LIMIT TO (article, customer, store, sale) FROM SERVER stage INTO stage_fdw'
