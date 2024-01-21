#!/bin/bash

# Stop docker
docker compose down

# Confirm deletion of all parquet data
echo ""
echo "Parquet data:"
du -h data/
echo ""
echo -n "--> Delete all parquet data (y/n)? "
read yn
if [ "$yn" = "y" ] ; then
    find data/* -maxdepth 1 -type d -exec rm -rf -v {} +
fi
echo ""