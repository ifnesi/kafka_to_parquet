#!/bin/bash

# Stop docker
docker compose down

# Confirm deletion of all parquet data
echo "Delete all parquet data (y/n)?"
read yn
if [ "$yn" = "y" ] ; then
    rm -rf data/*
fi
