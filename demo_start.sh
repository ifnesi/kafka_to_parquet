#!/bin/bash
docker compose up -d --build

# Waiting services to be ready
echo ""
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8081)" != "200" ]]
do
    echo "Waiting Schema Registry to be ready..."
    sleep 2
done

echo ""
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083)" != "200" ]]
do
    echo "Waiting Connect Cluster to be ready..."
    sleep 2
done

echo ""
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:9021)" != "200" ]]
do
    echo "Waiting Confluent Control Center to be ready..."
    sleep 2
done

# Create Datagen connectors
echo ""
echo "Creating Datagen connector (Stock Trading)"
curl -i -X PUT http://localhost:8083/connectors/datagen_stock_trade/config \
     -H "Content-Type: application/json" \
     -d '{
            "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "kafka.topic": "stock_trade",
            "schema.string": "{\"namespace\": \"ksql\", \"name\": \"StockTrade\", \"type\": \"record\", \"fields\": [{\"name\": \"side\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"options\": [\"BUY\", \"SELL\"]}}}, {\"name\": \"quantity\", \"type\": {\"type\": \"int\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 10}}}}, {\"name\": \"symbol\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"STK_[0-9]\"}}}, {\"name\": \"price\", \"type\": {\"type\": \"double\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 25}}}}, {\"name\": \"account\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"Account_[0-9]{2}\"}}}, {\"name\": \"userid\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"User_[0-9]{2}\"}}}]}",
            "max.interval": 100,
            "iterations": 10000000,
            "tasks.max": "1"
        }'
sleep 2

echo ""
echo "Creating Datagen connector (Purchase)"
curl -i -X PUT http://localhost:8083/connectors/datagen_purchase/config \
     -H "Content-Type: application/json" \
     -d '{
            "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "kafka.topic": "purchase",
            "schema.string": "{\"namespace\": \"ksql\", \"name\": \"Purchase\", \"type\": \"record\", \"fields\": [{\"name\": \"quantity\", \"type\": {\"type\": \"int\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 10}}}}, {\"name\": \"sku\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"sku_[0-8]\"}}}, {\"name\": \"price\", \"type\": {\"type\": \"double\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 100}}}}, {\"name\": \"storeid\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"Store_[0-2]\"}}}]}",
            "max.interval": 100,
            "iterations": 10000000,
            "tasks.max": "1"
        }'
sleep 2

# Check connector statuses
echo ""
echo ""
echo "Datagen connector status (Stock Trading)"
curl -s http://localhost:8083/connectors/datagen_stock_trade/status
sleep 1

echo ""
echo ""
echo "Datagen connector status (Stock Trading)"
curl -s http://localhost:8083/connectors/datagen_purchase/status
sleep 1

echo ""
echo ""
echo "Demo environment is ready!"
echo "Confluent Control Center -> http://localhost:9021"
echo ""

source .venv/bin/activate
python3 kafka_to_parquet.py --topics stock_trade purchase \
    --offset-reset earliest \
    --config-filename localhost.ini \
    --dump-records 200 \
    --dump-timeout 60 \
    --delete