![image](static/docs/confluent-logo-300-2.png)

# Kafka to Parquet

This is a very basic and rudimentar python Kafka consumer to batch topic data (Avro serialised) and dump to [Apache Parquet](https://parquet.apache.org/) files (one table per topic).

Parquet files will be saved under the folder `data/` as `database_EPOCH/`.

## Requirements:
- curl
- DuckDB
- Docker Desktop
- Python 3.8+
- Install python requirements (`python3 -m pip install -r requirements.txt`)

## Demo Diagram
- Data will be produced by two [DataGen source connectors](https://docs.confluent.io/kafka-connectors/datagen/current/overview.html):
  - datagen_stock_trade: e.g. `{"side": "BUY", "quantity": 4, "symbol": "STK_9", "price": 8.187322875120408, "account": "Account_63", "userid": "User_60"}`
  - datagen_purchase: e.g. `{"quantity": 8, "sku": "sku_3", "price": 53.5951224569137, "storeid": "Store_0"}`
- Python consumer `kafka_to_parquet.py` will batch events (in lots of 200 by default) and dump to Parquet files (one table per topic)
- Python script `data_analytics.py` will load the Parquet files and generate analytics dashboard

![image](static/docs/demo_diagram.png)

# Running the demo
To automatically setup the demo, run `./demo_start.sh`, it should take less than 2 minutes to have everything up and running.
```
 ✔ Network kafka_to_parquet_default  Created
 ✔ Container zookeeper               Started
 ✔ Container broker                  Started
 ✔ Container schema-registry         Started
 ✔ Container connect                 Started
 ✔ Container control-center          Started

Waiting Schema Registry to be ready...
Waiting Schema Registry to be ready...
Waiting Schema Registry to be ready...
Waiting Schema Registry to be ready...
Waiting Schema Registry to be ready...

Waiting Connect Cluster to be ready...
Waiting Connect Cluster to be ready...
Waiting Connect Cluster to be ready...
Waiting Connect Cluster to be ready...
Waiting Connect Cluster to be ready...
Waiting Connect Cluster to be ready...

Creating Datagen connector (Stock Trading)
HTTP/1.1 201 Created
Date: Sat, 20 Jan 2024 15:32:36 GMT
Location: http://localhost:8083/connectors/datagen_stock_trade
Content-Type: application/json
Content-Length: 1107

{"name":"datagen_stock_trade","config":{"connector.class":"io.confluent.kafka.connect.datagen.DatagenConnector","key.converter":"org.apache.kafka.connect.storage.StringConverter","kafka.topic":"stock_trade","schema.string":"{\"namespace\": \"ksql\", \"name\": \"StockTrade\", \"type\": \"record\", \"fields\": [{\"name\": \"side\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"options\": [\"BUY\", \"SELL\"]}}}, {\"name\": \"quantity\", \"type\": {\"type\": \"int\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 10}}}}, {\"name\": \"symbol\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"STK_[0-9]\"}}}, {\"name\": \"price\", \"type\": {\"type\": \"double\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 25}}}}, {\"name\": \"account\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"Account_[0-9]{2}\"}}}, {\"name\": \"userid\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"User_[0-9]{2}\"}}}]}","max.interval":"100","iterations":"10000000","tasks.max":"1","name":"datagen_stock_trade"},"tasks":[],"type":"source"}
Creating Datagen connector (Purchase)
HTTP/1.1 201 Created
Date: Sat, 20 Jan 2024 15:32:38 GMT
Location: http://localhost:8083/connectors/datagen_purchase
Content-Type: application/json
Content-Length: 867

{"name":"datagen_purchase","config":{"connector.class":"io.confluent.kafka.connect.datagen.DatagenConnector","key.converter":"org.apache.kafka.connect.storage.StringConverter","kafka.topic":"purchase","schema.string":"{\"namespace\": \"ksql\", \"name\": \"Purchase\", \"type\": \"record\", \"fields\": [{\"name\": \"quantity\", \"type\": {\"type\": \"int\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 10}}}}, {\"name\": \"sku\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"sku_[0-8]\"}}}, {\"name\": \"price\", \"type\": {\"type\": \"double\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 100}}}}, {\"name\": \"storeid\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"Store_[0-2]\"}}}]}","max.interval":"100","iterations":"10000000","tasks.max":"1","name":"datagen_purchase"},"tasks":[],"type":"source"}

Datagen connector status (Stock Trading)
{"name":"datagen_stock_trade","connector":{"state":"RUNNING","worker_id":"connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"connect:8083"}],"type":"source"}

Datagen connector status (Stock Trading)
{"name":"datagen_purchase","connector":{"state":"RUNNING","worker_id":"connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"connect:8083"}],"type":"source"}

Demo environment is ready!
Confluent Control Center -> http://localhost:9021

2024-01-20 15:32:43.684 [INFO]: Started consumer parquet-demo-01 (parquet-demo) on topic(s): stock_trade, purchase'
2024-01-20 15:32:43.841 [INFO]: Received message #1 [stock_trade]: {"side": "BUY", "quantity": 3, "symbol": "STK_4", "price": 1.5467463090841678, "account": "Account_88", "userid": "User_73", "__ts": "2024-01-20T15:32:37.156000"}
2024-01-20 15:32:43.841 [INFO]: Received message #2 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_5", "price": 24.622546307131252, "account": "Account_97", "userid": "User_12", "__ts": "2024-01-20T15:32:37.204000"}
2024-01-20 15:32:43.841 [INFO]: Received message #3 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_9", "price": 11.200347521170455, "account": "Account_11", "userid": "User_82", "__ts": "2024-01-20T15:32:37.279000"}
2024-01-20 15:32:43.841 [INFO]: Received message #4 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_7", "price": 11.276738062053616, "account": "Account_04", "userid": "User_52", "__ts": "2024-01-20T15:32:37.317000"}
2024-01-20 15:32:43.841 [INFO]: Received message #5 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_7", "price": 6.336293140011754, "account": "Account_55", "userid": "User_64", "__ts": "2024-01-20T15:32:37.403000"}
2024-01-20 15:32:43.841 [INFO]: Received message #6 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_7", "price": 17.74706187230413, "account": "Account_03", "userid": "User_99", "__ts": "2024-01-20T15:32:37.466000"}
2024-01-20 15:32:43.841 [INFO]: Received message #7 [stock_trade]: {"side": "BUY", "quantity": 3, "symbol": "STK_0", "price": 12.089502618345954, "account": "Account_34", "userid": "User_32", "__ts": "2024-01-20T15:32:37.568000"}
2024-01-20 15:32:43.841 [INFO]: Received message #8 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_3", "price": 3.383965401175532, "account": "Account_53", "userid": "User_67", "__ts": "2024-01-20T15:32:37.596000"}
2024-01-20 15:32:43.841 [INFO]: Received message #9 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_7", "price": 5.992113812898009, "account": "Account_10", "userid": "User_31", "__ts": "2024-01-20T15:32:37.688000"}
2024-01-20 15:32:43.841 [INFO]: Received message #10 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_5", "price": 7.681567777319409, "account": "Account_15", "userid": "User_23", "__ts": "2024-01-20T15:32:37.732000"}
2024-01-20 15:32:43.841 [INFO]: Received message #11 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_3", "price": 6.633531387015501, "account": "Account_29", "userid": "User_62", "__ts": "2024-01-20T15:32:37.745000"}
2024-01-20 15:32:43.841 [INFO]: Received message #12 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_5", "price": 11.571871503077798, "account": "Account_58", "userid": "User_56", "__ts": "2024-01-20T15:32:37.817000"}
2024-01-20 15:32:43.841 [INFO]: Received message #13 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_4", "price": 15.632467237137437, "account": "Account_11", "userid": "User_80", "__ts": "2024-01-20T15:32:37.907000"}
2024-01-20 15:32:43.841 [INFO]: Received message #14 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_4", "price": 8.25326877578129, "account": "Account_33", "userid": "User_51", "__ts": "2024-01-20T15:32:37.934000"}
2024-01-20 15:32:43.841 [INFO]: Received message #15 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_8", "price": 3.576379284184557, "account": "Account_46", "userid": "User_64", "__ts": "2024-01-20T15:32:37.996000"}
2024-01-20 15:32:43.841 [INFO]: Received message #16 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_2", "price": 1.6946723507876724, "account": "Account_49", "userid": "User_02", "__ts": "2024-01-20T15:32:38.054000"}
2024-01-20 15:32:43.841 [INFO]: Received message #17 [stock_trade]: {"side": "BUY", "quantity": 7, "symbol": "STK_7", "price": 18.3580706177204, "account": "Account_32", "userid": "User_31", "__ts": "2024-01-20T15:32:38.084000"}
2024-01-20 15:32:43.841 [INFO]: Received message #18 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_5", "price": 4.591473396439897, "account": "Account_73", "userid": "User_71", "__ts": "2024-01-20T15:32:38.113000"}
2024-01-20 15:32:43.841 [INFO]: Received message #19 [stock_trade]: {"side": "BUY", "quantity": 7, "symbol": "STK_1", "price": 2.5305262367665193, "account": "Account_00", "userid": "User_21", "__ts": "2024-01-20T15:32:38.164000"}
2024-01-20 15:32:43.841 [INFO]: Received message #20 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_3", "price": 12.587305042593236, "account": "Account_18", "userid": "User_57", "__ts": "2024-01-20T15:32:38.195000"}
2024-01-20 15:32:43.841 [INFO]: Received message #21 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_2", "price": 20.109677889862915, "account": "Account_19", "userid": "User_68", "__ts": "2024-01-20T15:32:38.197000"}
2024-01-20 15:32:43.841 [INFO]: Received message #22 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_7", "price": 8.516828962803439, "account": "Account_93", "userid": "User_69", "__ts": "2024-01-20T15:32:38.269000"}
2024-01-20 15:32:43.841 [INFO]: Received message #23 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_9", "price": 2.967673028614832, "account": "Account_91", "userid": "User_85", "__ts": "2024-01-20T15:32:38.285000"}
2024-01-20 15:32:43.841 [INFO]: Received message #24 [stock_trade]: {"side": "BUY", "quantity": 9, "symbol": "STK_6", "price": 1.1323942246588308, "account": "Account_08", "userid": "User_47", "__ts": "2024-01-20T15:32:38.315000"}
2024-01-20 15:32:43.841 [INFO]: Received message #25 [stock_trade]: {"side": "BUY", "quantity": 4, "symbol": "STK_7", "price": 21.84170457036486, "account": "Account_64", "userid": "User_19", "__ts": "2024-01-20T15:32:38.336000"}
2024-01-20 15:32:43.841 [INFO]: Received message #26 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_9", "price": 23.358002915270465, "account": "Account_41", "userid": "User_39", "__ts": "2024-01-20T15:32:38.437000"}
2024-01-20 15:32:43.841 [INFO]: Received message #27 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_0", "price": 10.667625012054586, "account": "Account_59", "userid": "User_19", "__ts": "2024-01-20T15:32:38.452000"}
2024-01-20 15:32:43.842 [INFO]: Received message #28 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_9", "price": 7.880924856012528, "account": "Account_13", "userid": "User_22", "__ts": "2024-01-20T15:32:38.510000"}
2024-01-20 15:32:43.842 [INFO]: Received message #29 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_3", "price": 11.47685841454848, "account": "Account_65", "userid": "User_79", "__ts": "2024-01-20T15:32:38.593000"}
2024-01-20 15:32:43.842 [INFO]: Received message #30 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_9", "price": 4.036101291000772, "account": "Account_50", "userid": "User_11", "__ts": "2024-01-20T15:32:38.640000"}
2024-01-20 15:32:43.842 [INFO]: Received message #31 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_4", "price": 9.288132888547333, "account": "Account_12", "userid": "User_89", "__ts": "2024-01-20T15:32:38.726000"}
2024-01-20 15:32:43.842 [INFO]: Received message #32 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_2", "price": 6.359143817935217, "account": "Account_61", "userid": "User_96", "__ts": "2024-01-20T15:32:38.797000"}
2024-01-20 15:32:43.842 [INFO]: Received message #33 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_8", "price": 15.34207586170005, "account": "Account_80", "userid": "User_10", "__ts": "2024-01-20T15:32:38.849000"}
2024-01-20 15:32:43.842 [INFO]: Received message #34 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_6", "price": 6.679321765587078, "account": "Account_30", "userid": "User_00", "__ts": "2024-01-20T15:32:38.926000"}
2024-01-20 15:32:43.842 [INFO]: Received message #35 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_8", "price": 24.254284729004848, "account": "Account_97", "userid": "User_61", "__ts": "2024-01-20T15:32:39.019000"}
2024-01-20 15:32:43.842 [INFO]: Received message #36 [stock_trade]: {"side": "BUY", "quantity": 9, "symbol": "STK_7", "price": 9.854951130886597, "account": "Account_59", "userid": "User_82", "__ts": "2024-01-20T15:32:39.053000"}
2024-01-20 15:32:43.842 [INFO]: Received message #37 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_0", "price": 9.998905430605902, "account": "Account_48", "userid": "User_42", "__ts": "2024-01-20T15:32:39.072000"}
2024-01-20 15:32:43.842 [INFO]: Received message #38 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_7", "price": 19.120697280820057, "account": "Account_32", "userid": "User_99", "__ts": "2024-01-20T15:32:39.111000"}
2024-01-20 15:32:43.842 [INFO]: Received message #39 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_5", "price": 21.787651957792193, "account": "Account_40", "userid": "User_65", "__ts": "2024-01-20T15:32:39.121000"}
2024-01-20 15:32:43.842 [INFO]: Received message #40 [stock_trade]: {"side": "BUY", "quantity": 2, "symbol": "STK_2", "price": 20.24181212237678, "account": "Account_42", "userid": "User_60", "__ts": "2024-01-20T15:32:39.131000"}
2024-01-20 15:32:43.842 [INFO]: Received message #41 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_1", "price": 6.502019315230025, "account": "Account_86", "userid": "User_52", "__ts": "2024-01-20T15:32:39.224000"}
2024-01-20 15:32:43.842 [INFO]: Received message #42 [stock_trade]: {"side": "BUY", "quantity": 3, "symbol": "STK_2", "price": 23.162091319362222, "account": "Account_01", "userid": "User_69", "__ts": "2024-01-20T15:32:39.308000"}
2024-01-20 15:32:43.842 [INFO]: Received message #43 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_2", "price": 21.23684844272752, "account": "Account_25", "userid": "User_25", "__ts": "2024-01-20T15:32:39.370000"}
2024-01-20 15:32:43.842 [INFO]: Received message #44 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_3", "price": 15.85497398978205, "account": "Account_86", "userid": "User_73", "__ts": "2024-01-20T15:32:39.400000"}
2024-01-20 15:32:43.842 [INFO]: Received message #45 [stock_trade]: {"side": "BUY", "quantity": 4, "symbol": "STK_9", "price": 18.39264163972002, "account": "Account_07", "userid": "User_82", "__ts": "2024-01-20T15:32:39.500000"}
2024-01-20 15:32:43.842 [INFO]: Received message #46 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_1", "price": 14.6479089273958, "account": "Account_51", "userid": "User_51", "__ts": "2024-01-20T15:32:39.514000"}
2024-01-20 15:32:43.842 [INFO]: Received message #47 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_6", "price": 3.7591106456383434, "account": "Account_89", "userid": "User_98", "__ts": "2024-01-20T15:32:39.569000"}
2024-01-20 15:32:43.842 [INFO]: Received message #48 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_6", "price": 8.907031372599516, "account": "Account_81", "userid": "User_53", "__ts": "2024-01-20T15:32:39.583000"}
2024-01-20 15:32:43.842 [INFO]: Received message #49 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_7", "price": 20.394555386186468, "account": "Account_77", "userid": "User_52", "__ts": "2024-01-20T15:32:39.679000"}
2024-01-20 15:32:43.842 [INFO]: Received message #50 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_9", "price": 11.99841518444098, "account": "Account_35", "userid": "User_02", "__ts": "2024-01-20T15:32:39.745000"}
2024-01-20 15:32:43.842 [INFO]: Received message #51 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_3", "price": 21.253291240553708, "account": "Account_81", "userid": "User_20", "__ts": "2024-01-20T15:32:39.845000"}
2024-01-20 15:32:43.842 [INFO]: Received message #52 [stock_trade]: {"side": "BUY", "quantity": 7, "symbol": "STK_2", "price": 17.169829185092553, "account": "Account_07", "userid": "User_50", "__ts": "2024-01-20T15:32:39.876000"}
2024-01-20 15:32:43.842 [INFO]: Received message #53 [stock_trade]: {"side": "BUY", "quantity": 2, "symbol": "STK_9", "price": 10.021805825820849, "account": "Account_35", "userid": "User_94", "__ts": "2024-01-20T15:32:39.951000"}
2024-01-20 15:32:43.842 [INFO]: Received message #54 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_3", "price": 14.79595442654238, "account": "Account_08", "userid": "User_08", "__ts": "2024-01-20T15:32:40.024000"}
2024-01-20 15:32:43.842 [INFO]: Received message #55 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_8", "price": 20.782920632960142, "account": "Account_42", "userid": "User_01", "__ts": "2024-01-20T15:32:40.118000"}
2024-01-20 15:32:43.842 [INFO]: Received message #56 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_4", "price": 13.035960767769636, "account": "Account_88", "userid": "User_26", "__ts": "2024-01-20T15:32:40.119000"}
2024-01-20 15:32:43.842 [INFO]: Received message #57 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_4", "price": 4.3772311527019525, "account": "Account_35", "userid": "User_35", "__ts": "2024-01-20T15:32:40.165000"}
2024-01-20 15:32:43.842 [INFO]: Received message #58 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_7", "price": 8.687877551842254, "account": "Account_67", "userid": "User_59", "__ts": "2024-01-20T15:32:40.237000"}
2024-01-20 15:32:43.843 [INFO]: Received message #59 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_9", "price": 17.333543951796102, "account": "Account_91", "userid": "User_03", "__ts": "2024-01-20T15:32:40.245000"}
2024-01-20 15:32:43.843 [INFO]: Received message #60 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_9", "price": 18.217306017803438, "account": "Account_69", "userid": "User_80", "__ts": "2024-01-20T15:32:40.273000"}
2024-01-20 15:32:43.843 [INFO]: Received message #61 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_4", "price": 18.245635479387783, "account": "Account_25", "userid": "User_84", "__ts": "2024-01-20T15:32:40.365000"}
2024-01-20 15:32:43.843 [INFO]: Received message #62 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_9", "price": 6.606969437828363, "account": "Account_76", "userid": "User_71", "__ts": "2024-01-20T15:32:40.399000"}
2024-01-20 15:32:43.843 [INFO]: Received message #63 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_9", "price": 12.608515259392224, "account": "Account_88", "userid": "User_85", "__ts": "2024-01-20T15:32:40.473000"}
2024-01-20 15:32:43.843 [INFO]: Received message #64 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_8", "price": 2.2677994244639494, "account": "Account_22", "userid": "User_53", "__ts": "2024-01-20T15:32:40.494000"}
2024-01-20 15:32:43.843 [INFO]: Received message #65 [stock_trade]: {"side": "BUY", "quantity": 2, "symbol": "STK_2", "price": 7.848869293612397, "account": "Account_09", "userid": "User_29", "__ts": "2024-01-20T15:32:40.513000"}
2024-01-20 15:32:43.843 [INFO]: Received message #66 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_9", "price": 13.19785639057374, "account": "Account_11", "userid": "User_02", "__ts": "2024-01-20T15:32:40.570000"}
2024-01-20 15:32:43.843 [INFO]: Received message #67 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_4", "price": 15.739254658439323, "account": "Account_95", "userid": "User_05", "__ts": "2024-01-20T15:32:40.594000"}
2024-01-20 15:32:43.843 [INFO]: Received message #68 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_4", "price": 5.397570273557586, "account": "Account_75", "userid": "User_34", "__ts": "2024-01-20T15:32:40.681000"}
2024-01-20 15:32:43.843 [INFO]: Received message #69 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_0", "price": 19.61959907138114, "account": "Account_22", "userid": "User_42", "__ts": "2024-01-20T15:32:40.716000"}
2024-01-20 15:32:43.843 [INFO]: Received message #70 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_8", "price": 23.54098833254577, "account": "Account_90", "userid": "User_42", "__ts": "2024-01-20T15:32:40.791000"}
2024-01-20 15:32:43.843 [INFO]: Received message #71 [stock_trade]: {"side": "BUY", "quantity": 3, "symbol": "STK_9", "price": 3.3247225087505106, "account": "Account_30", "userid": "User_67", "__ts": "2024-01-20T15:32:40.816000"}
2024-01-20 15:32:43.843 [INFO]: Received message #72 [stock_trade]: {"side": "BUY", "quantity": 4, "symbol": "STK_7", "price": 3.328969006749925, "account": "Account_40", "userid": "User_99", "__ts": "2024-01-20T15:32:40.822000"}
2024-01-20 15:32:43.843 [INFO]: Received message #73 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_9", "price": 20.77155300328024, "account": "Account_35", "userid": "User_27", "__ts": "2024-01-20T15:32:40.889000"}
2024-01-20 15:32:43.843 [INFO]: Received message #74 [stock_trade]: {"side": "BUY", "quantity": 4, "symbol": "STK_1", "price": 2.5752482915298254, "account": "Account_26", "userid": "User_98", "__ts": "2024-01-20T15:32:40.928000"}
2024-01-20 15:32:43.843 [INFO]: Received message #75 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_3", "price": 8.852346796936638, "account": "Account_68", "userid": "User_58", "__ts": "2024-01-20T15:32:40.947000"}
2024-01-20 15:32:43.843 [INFO]: Received message #76 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_5", "price": 15.838179846643552, "account": "Account_59", "userid": "User_59", "__ts": "2024-01-20T15:32:41.041000"}
2024-01-20 15:32:43.843 [INFO]: Received message #77 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_3", "price": 24.531129743668917, "account": "Account_32", "userid": "User_73", "__ts": "2024-01-20T15:32:41.061000"}
2024-01-20 15:32:43.843 [INFO]: Received message #78 [stock_trade]: {"side": "BUY", "quantity": 1, "symbol": "STK_7", "price": 19.452163586270515, "account": "Account_52", "userid": "User_17", "__ts": "2024-01-20T15:32:41.147000"}
2024-01-20 15:32:43.843 [INFO]: Received message #79 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_3", "price": 17.964966182670487, "account": "Account_93", "userid": "User_49", "__ts": "2024-01-20T15:32:41.221000"}
2024-01-20 15:32:43.844 [INFO]: Received message #80 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_2", "price": 5.7363853245544885, "account": "Account_62", "userid": "User_29", "__ts": "2024-01-20T15:32:41.232000"}
2024-01-20 15:32:43.844 [INFO]: Received message #81 [stock_trade]: {"side": "BUY", "quantity": 6, "symbol": "STK_1", "price": 6.38599382146973, "account": "Account_30", "userid": "User_29", "__ts": "2024-01-20T15:32:41.244000"}
2024-01-20 15:32:43.844 [INFO]: Received message #82 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_2", "price": 24.464654543484027, "account": "Account_57", "userid": "User_21", "__ts": "2024-01-20T15:32:41.326000"}
2024-01-20 15:32:43.844 [INFO]: Received message #83 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_7", "price": 9.21108478984809, "account": "Account_83", "userid": "User_25", "__ts": "2024-01-20T15:32:41.390000"}
2024-01-20 15:32:43.844 [INFO]: Received message #84 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_5", "price": 22.99699193307888, "account": "Account_95", "userid": "User_37", "__ts": "2024-01-20T15:32:41.412000"}
2024-01-20 15:32:43.844 [INFO]: Received message #85 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_5", "price": 9.793940239124655, "account": "Account_30", "userid": "User_45", "__ts": "2024-01-20T15:32:41.502000"}
2024-01-20 15:32:43.844 [INFO]: Received message #86 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_8", "price": 21.557050914164538, "account": "Account_62", "userid": "User_60", "__ts": "2024-01-20T15:32:41.546000"}
2024-01-20 15:32:43.844 [INFO]: Received message #87 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_6", "price": 21.22622990843367, "account": "Account_06", "userid": "User_24", "__ts": "2024-01-20T15:32:41.587000"}
2024-01-20 15:32:43.844 [INFO]: Received message #88 [stock_trade]: {"side": "BUY", "quantity": 2, "symbol": "STK_4", "price": 1.142485718762651, "account": "Account_49", "userid": "User_01", "__ts": "2024-01-20T15:32:41.641000"}
2024-01-20 15:32:43.844 [INFO]: Received message #89 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_7", "price": 20.577948229895853, "account": "Account_32", "userid": "User_08", "__ts": "2024-01-20T15:32:41.720000"}
2024-01-20 15:32:43.844 [INFO]: Received message #90 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_3", "price": 1.487200934256581, "account": "Account_90", "userid": "User_50", "__ts": "2024-01-20T15:32:41.759000"}
2024-01-20 15:32:43.844 [INFO]: Received message #91 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_4", "price": 1.955373054593446, "account": "Account_22", "userid": "User_42", "__ts": "2024-01-20T15:32:41.844000"}
2024-01-20 15:32:43.844 [INFO]: Received message #92 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_6", "price": 3.444925411221077, "account": "Account_78", "userid": "User_55", "__ts": "2024-01-20T15:32:41.922000"}
2024-01-20 15:32:43.844 [INFO]: Received message #93 [stock_trade]: {"side": "BUY", "quantity": 4, "symbol": "STK_4", "price": 6.685531025565982, "account": "Account_86", "userid": "User_52", "__ts": "2024-01-20T15:32:42.020000"}
2024-01-20 15:32:43.844 [INFO]: Received message #94 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_2", "price": 3.905573364622307, "account": "Account_08", "userid": "User_42", "__ts": "2024-01-20T15:32:42.061000"}
2024-01-20 15:32:43.844 [INFO]: Received message #95 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_1", "price": 2.034090160777975, "account": "Account_52", "userid": "User_62", "__ts": "2024-01-20T15:32:42.094000"}
2024-01-20 15:32:43.844 [INFO]: Received message #96 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_7", "price": 18.907832501174994, "account": "Account_91", "userid": "User_69", "__ts": "2024-01-20T15:32:42.184000"}
2024-01-20 15:32:43.844 [INFO]: Received message #97 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_8", "price": 24.43299207262055, "account": "Account_36", "userid": "User_14", "__ts": "2024-01-20T15:32:42.258000"}
2024-01-20 15:32:43.844 [INFO]: Received message #98 [stock_trade]: {"side": "BUY", "quantity": 3, "symbol": "STK_0", "price": 2.8714272545352424, "account": "Account_58", "userid": "User_45", "__ts": "2024-01-20T15:32:42.348000"}
2024-01-20 15:32:43.844 [INFO]: Received message #99 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_4", "price": 1.1542812853349114, "account": "Account_16", "userid": "User_41", "__ts": "2024-01-20T15:32:42.409000"}
2024-01-20 15:32:43.844 [INFO]: Received message #100 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_1", "price": 17.98673249376885, "account": "Account_79", "userid": "User_69", "__ts": "2024-01-20T15:32:42.489000"}
2024-01-20 15:32:43.844 [INFO]: Received message #101 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_9", "price": 11.459115115776974, "account": "Account_36", "userid": "User_65", "__ts": "2024-01-20T15:32:42.502000"}
2024-01-20 15:32:43.844 [INFO]: Received message #102 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_2", "price": 24.644841037304797, "account": "Account_24", "userid": "User_93", "__ts": "2024-01-20T15:32:42.526000"}
2024-01-20 15:32:43.844 [INFO]: Received message #103 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_4", "price": 4.777067804832544, "account": "Account_39", "userid": "User_09", "__ts": "2024-01-20T15:32:42.571000"}
2024-01-20 15:32:43.844 [INFO]: Received message #104 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_9", "price": 21.856649991062113, "account": "Account_30", "userid": "User_57", "__ts": "2024-01-20T15:32:42.661000"}
2024-01-20 15:32:43.844 [INFO]: Received message #105 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_7", "price": 11.884101492060617, "account": "Account_49", "userid": "User_21", "__ts": "2024-01-20T15:32:42.758000"}
2024-01-20 15:32:43.844 [INFO]: Received message #106 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_3", "price": 22.463869627102124, "account": "Account_36", "userid": "User_70", "__ts": "2024-01-20T15:32:42.861000"}
2024-01-20 15:32:43.844 [INFO]: Received message #107 [stock_trade]: {"side": "SELL", "quantity": 1, "symbol": "STK_5", "price": 15.482102893036409, "account": "Account_83", "userid": "User_09", "__ts": "2024-01-20T15:32:42.918000"}
2024-01-20 15:32:43.844 [INFO]: Received message #108 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_4", "price": 4.791350129812953, "account": "Account_02", "userid": "User_38", "__ts": "2024-01-20T15:32:42.980000"}
2024-01-20 15:32:43.844 [INFO]: Received message #109 [stock_trade]: {"side": "BUY", "quantity": 7, "symbol": "STK_3", "price": 13.22672072779781, "account": "Account_87", "userid": "User_73", "__ts": "2024-01-20T15:32:42.992000"}
2024-01-20 15:32:43.844 [INFO]: Received message #110 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_9", "price": 9.598422403883779, "account": "Account_83", "userid": "User_95", "__ts": "2024-01-20T15:32:43.052000"}
2024-01-20 15:32:43.844 [INFO]: Received message #111 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_9", "price": 7.9606099143081686, "account": "Account_20", "userid": "User_21", "__ts": "2024-01-20T15:32:43.133000"}
2024-01-20 15:32:43.844 [INFO]: Received message #112 [stock_trade]: {"side": "BUY", "quantity": 1, "symbol": "STK_3", "price": 21.79734787885636, "account": "Account_81", "userid": "User_26", "__ts": "2024-01-20T15:32:43.205000"}
2024-01-20 15:32:43.844 [INFO]: Received message #113 [stock_trade]: {"side": "SELL", "quantity": 3, "symbol": "STK_0", "price": 13.469667975868294, "account": "Account_36", "userid": "User_35", "__ts": "2024-01-20T15:32:43.265000"}
2024-01-20 15:32:43.844 [INFO]: Received message #114 [stock_trade]: {"side": "BUY", "quantity": 7, "symbol": "STK_8", "price": 2.805268502721318, "account": "Account_65", "userid": "User_90", "__ts": "2024-01-20T15:32:43.273000"}
2024-01-20 15:32:43.844 [INFO]: Received message #115 [stock_trade]: {"side": "BUY", "quantity": 1, "symbol": "STK_5", "price": 23.865188037019877, "account": "Account_30", "userid": "User_43", "__ts": "2024-01-20T15:32:43.355000"}
2024-01-20 15:32:43.844 [INFO]: Received message #116 [stock_trade]: {"side": "BUY", "quantity": 9, "symbol": "STK_6", "price": 13.463810960627654, "account": "Account_54", "userid": "User_56", "__ts": "2024-01-20T15:32:43.421000"}
2024-01-20 15:32:43.844 [INFO]: Received message #117 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_7", "price": 6.169413774452237, "account": "Account_94", "userid": "User_13", "__ts": "2024-01-20T15:32:43.505000"}
2024-01-20 15:32:43.844 [INFO]: Received message #118 [stock_trade]: {"side": "SELL", "quantity": 4, "symbol": "STK_0", "price": 12.923035749009266, "account": "Account_88", "userid": "User_07", "__ts": "2024-01-20T15:32:43.572000"}
2024-01-20 15:32:43.844 [INFO]: Received message #119 [stock_trade]: {"side": "SELL", "quantity": 6, "symbol": "STK_1", "price": 13.356527904311214, "account": "Account_77", "userid": "User_43", "__ts": "2024-01-20T15:32:43.637000"}
2024-01-20 15:32:43.844 [INFO]: Received message #120 [stock_trade]: {"side": "BUY", "quantity": 1, "symbol": "STK_3", "price": 15.01789900518185, "account": "Account_61", "userid": "User_02", "__ts": "2024-01-20T15:32:43.706000"}
2024-01-20 15:32:43.844 [INFO]: Received message #121 [stock_trade]: {"side": "SELL", "quantity": 5, "symbol": "STK_9", "price": 13.168107620029502, "account": "Account_10", "userid": "User_19", "__ts": "2024-01-20T15:32:43.782000"}
2024-01-20 15:32:43.850 [INFO]: Received message #122 [purchase]: {"quantity": 7, "sku": "sku_5", "price": 81.64818498261333, "storeid": "Store_2", "__ts": "2024-01-20T15:32:38.854000"}
2024-01-20 15:32:43.850 [INFO]: Received message #123 [purchase]: {"quantity": 2, "sku": "sku_1", "price": 68.7111396782722, "storeid": "Store_2", "__ts": "2024-01-20T15:32:38.901000"}
2024-01-20 15:32:43.850 [INFO]: Received message #124 [purchase]: {"quantity": 5, "sku": "sku_4", "price": 67.87534091356267, "storeid": "Store_2", "__ts": "2024-01-20T15:32:38.947000"}
2024-01-20 15:32:43.850 [INFO]: Received message #125 [purchase]: {"quantity": 8, "sku": "sku_8", "price": 15.785003070436881, "storeid": "Store_0", "__ts": "2024-01-20T15:32:38.961000"}
2024-01-20 15:32:43.850 [INFO]: Received message #126 [purchase]: {"quantity": 6, "sku": "sku_3", "price": 45.371813042992024, "storeid": "Store_2", "__ts": "2024-01-20T15:32:38.984000"}
2024-01-20 15:32:43.850 [INFO]: Received message #127 [purchase]: {"quantity": 7, "sku": "sku_2", "price": 20.993221590368563, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.082000"}
2024-01-20 15:32:43.850 [INFO]: Received message #128 [purchase]: {"quantity": 8, "sku": "sku_4", "price": 70.04717760994646, "storeid": "Store_0", "__ts": "2024-01-20T15:32:39.151000"}
2024-01-20 15:32:43.850 [INFO]: Received message #129 [purchase]: {"quantity": 3, "sku": "sku_4", "price": 79.68525931701905, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.166000"}
2024-01-20 15:32:43.850 [INFO]: Received message #130 [purchase]: {"quantity": 9, "sku": "sku_2", "price": 96.15869680280649, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.237000"}
2024-01-20 15:32:43.850 [INFO]: Received message #131 [purchase]: {"quantity": 8, "sku": "sku_3", "price": 92.93220470568794, "storeid": "Store_0", "__ts": "2024-01-20T15:32:39.257000"}
2024-01-20 15:32:43.850 [INFO]: Received message #132 [purchase]: {"quantity": 5, "sku": "sku_4", "price": 34.339595682841065, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.274000"}
2024-01-20 15:32:43.850 [INFO]: Received message #133 [purchase]: {"quantity": 6, "sku": "sku_4", "price": 10.053945381068102, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.351000"}
2024-01-20 15:32:43.850 [INFO]: Received message #134 [purchase]: {"quantity": 9, "sku": "sku_5", "price": 44.41087270393617, "storeid": "Store_0", "__ts": "2024-01-20T15:32:39.419000"}
2024-01-20 15:32:43.850 [INFO]: Received message #135 [purchase]: {"quantity": 7, "sku": "sku_0", "price": 23.251672947007116, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.501000"}
2024-01-20 15:32:43.850 [INFO]: Received message #136 [purchase]: {"quantity": 3, "sku": "sku_7", "price": 37.827363906398276, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.600000"}
2024-01-20 15:32:43.850 [INFO]: Received message #137 [purchase]: {"quantity": 7, "sku": "sku_6", "price": 40.33505776742145, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.670000"}
2024-01-20 15:32:43.850 [INFO]: Received message #138 [purchase]: {"quantity": 2, "sku": "sku_8", "price": 90.07902372316863, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.680000"}
2024-01-20 15:32:43.850 [INFO]: Received message #139 [purchase]: {"quantity": 6, "sku": "sku_4", "price": 51.98865376404164, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.706000"}
2024-01-20 15:32:43.850 [INFO]: Received message #140 [purchase]: {"quantity": 6, "sku": "sku_8", "price": 42.01948420794479, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.761000"}
2024-01-20 15:32:43.850 [INFO]: Received message #141 [purchase]: {"quantity": 3, "sku": "sku_1", "price": 94.13680484053867, "storeid": "Store_2", "__ts": "2024-01-20T15:32:39.818000"}
2024-01-20 15:32:43.850 [INFO]: Received message #142 [purchase]: {"quantity": 2, "sku": "sku_7", "price": 27.31454202891616, "storeid": "Store_0", "__ts": "2024-01-20T15:32:39.897000"}
2024-01-20 15:32:43.850 [INFO]: Received message #143 [purchase]: {"quantity": 7, "sku": "sku_5", "price": 83.18371604886103, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.921000"}
2024-01-20 15:32:43.850 [INFO]: Received message #144 [purchase]: {"quantity": 8, "sku": "sku_8", "price": 11.033579173384677, "storeid": "Store_1", "__ts": "2024-01-20T15:32:39.942000"}
2024-01-20 15:32:43.850 [INFO]: Received message #145 [purchase]: {"quantity": 9, "sku": "sku_0", "price": 8.75653728456165, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.005000"}
2024-01-20 15:32:43.850 [INFO]: Received message #146 [purchase]: {"quantity": 7, "sku": "sku_4", "price": 86.42580258145955, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.068000"}
2024-01-20 15:32:43.850 [INFO]: Received message #147 [purchase]: {"quantity": 9, "sku": "sku_0", "price": 93.64514380078867, "storeid": "Store_0", "__ts": "2024-01-20T15:32:40.165000"}
2024-01-20 15:32:43.850 [INFO]: Received message #148 [purchase]: {"quantity": 1, "sku": "sku_7", "price": 79.36579300107961, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.228000"}
2024-01-20 15:32:43.850 [INFO]: Received message #149 [purchase]: {"quantity": 1, "sku": "sku_2", "price": 77.11227053572104, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.248000"}
2024-01-20 15:32:43.850 [INFO]: Received message #150 [purchase]: {"quantity": 3, "sku": "sku_0", "price": 39.514228708250776, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.332000"}
2024-01-20 15:32:43.850 [INFO]: Received message #151 [purchase]: {"quantity": 3, "sku": "sku_3", "price": 59.419689962885194, "storeid": "Store_2", "__ts": "2024-01-20T15:32:40.355000"}
2024-01-20 15:32:43.850 [INFO]: Received message #152 [purchase]: {"quantity": 7, "sku": "sku_0", "price": 36.76621004434329, "storeid": "Store_0", "__ts": "2024-01-20T15:32:40.361000"}
2024-01-20 15:32:43.850 [INFO]: Received message #153 [purchase]: {"quantity": 7, "sku": "sku_2", "price": 18.447454393993485, "storeid": "Store_0", "__ts": "2024-01-20T15:32:40.440000"}
2024-01-20 15:32:43.851 [INFO]: Received message #154 [purchase]: {"quantity": 9, "sku": "sku_5", "price": 97.057173053316, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.516000"}
2024-01-20 15:32:43.851 [INFO]: Received message #155 [purchase]: {"quantity": 8, "sku": "sku_7", "price": 6.853608592972684, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.551000"}
2024-01-20 15:32:43.851 [INFO]: Received message #156 [purchase]: {"quantity": 8, "sku": "sku_2", "price": 79.9409844472656, "storeid": "Store_0", "__ts": "2024-01-20T15:32:40.555000"}
2024-01-20 15:32:43.851 [INFO]: Received message #157 [purchase]: {"quantity": 7, "sku": "sku_7", "price": 40.20406526521046, "storeid": "Store_2", "__ts": "2024-01-20T15:32:40.636000"}
2024-01-20 15:32:43.851 [INFO]: Received message #158 [purchase]: {"quantity": 3, "sku": "sku_5", "price": 84.02450786173836, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.670000"}
2024-01-20 15:32:43.851 [INFO]: Received message #159 [purchase]: {"quantity": 7, "sku": "sku_5", "price": 84.69681674796391, "storeid": "Store_2", "__ts": "2024-01-20T15:32:40.685000"}
2024-01-20 15:32:43.851 [INFO]: Received message #160 [purchase]: {"quantity": 1, "sku": "sku_7", "price": 47.18348458933731, "storeid": "Store_2", "__ts": "2024-01-20T15:32:40.701000"}
2024-01-20 15:32:43.851 [INFO]: Received message #161 [purchase]: {"quantity": 2, "sku": "sku_5", "price": 19.06497389560589, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.737000"}
2024-01-20 15:32:43.851 [INFO]: Received message #162 [purchase]: {"quantity": 3, "sku": "sku_2", "price": 5.496976668502624, "storeid": "Store_2", "__ts": "2024-01-20T15:32:40.746000"}
2024-01-20 15:32:43.851 [INFO]: Received message #163 [purchase]: {"quantity": 3, "sku": "sku_6", "price": 4.00567947059924, "storeid": "Store_2", "__ts": "2024-01-20T15:32:40.845000"}
2024-01-20 15:32:43.851 [INFO]: Received message #164 [purchase]: {"quantity": 9, "sku": "sku_6", "price": 79.44243098081526, "storeid": "Store_1", "__ts": "2024-01-20T15:32:40.903000"}
2024-01-20 15:32:43.851 [INFO]: Received message #165 [purchase]: {"quantity": 6, "sku": "sku_1", "price": 29.21860872181241, "storeid": "Store_0", "__ts": "2024-01-20T15:32:40.995000"}
2024-01-20 15:32:43.851 [INFO]: Received message #166 [purchase]: {"quantity": 3, "sku": "sku_0", "price": 41.02747530293609, "storeid": "Store_1", "__ts": "2024-01-20T15:32:41.054000"}
2024-01-20 15:32:43.851 [INFO]: Received message #167 [purchase]: {"quantity": 7, "sku": "sku_7", "price": 99.08451278799403, "storeid": "Store_1", "__ts": "2024-01-20T15:32:41.082000"}
2024-01-20 15:32:43.851 [INFO]: Received message #168 [purchase]: {"quantity": 5, "sku": "sku_7", "price": 50.75262850963915, "storeid": "Store_1", "__ts": "2024-01-20T15:32:41.185000"}
2024-01-20 15:32:43.851 [INFO]: Received message #169 [purchase]: {"quantity": 5, "sku": "sku_5", "price": 11.739294998506058, "storeid": "Store_0", "__ts": "2024-01-20T15:32:41.202000"}
2024-01-20 15:32:43.851 [INFO]: Received message #170 [purchase]: {"quantity": 8, "sku": "sku_4", "price": 62.443813097887414, "storeid": "Store_2", "__ts": "2024-01-20T15:32:41.276000"}
2024-01-20 15:32:43.851 [INFO]: Received message #171 [purchase]: {"quantity": 3, "sku": "sku_3", "price": 70.91189998281759, "storeid": "Store_0", "__ts": "2024-01-20T15:32:41.326000"}
2024-01-20 15:32:43.851 [INFO]: Received message #172 [purchase]: {"quantity": 8, "sku": "sku_7", "price": 35.44233031892504, "storeid": "Store_0", "__ts": "2024-01-20T15:32:41.388000"}
2024-01-20 15:32:43.851 [INFO]: Received message #173 [purchase]: {"quantity": 8, "sku": "sku_8", "price": 65.8732102805759, "storeid": "Store_1", "__ts": "2024-01-20T15:32:41.490000"}
2024-01-20 15:32:43.851 [INFO]: Received message #174 [purchase]: {"quantity": 4, "sku": "sku_1", "price": 9.99706533362932, "storeid": "Store_0", "__ts": "2024-01-20T15:32:41.506000"}
2024-01-20 15:32:43.851 [INFO]: Received message #175 [purchase]: {"quantity": 4, "sku": "sku_2", "price": 92.92847453205837, "storeid": "Store_1", "__ts": "2024-01-20T15:32:41.587000"}
2024-01-20 15:32:43.851 [INFO]: Received message #176 [purchase]: {"quantity": 4, "sku": "sku_8", "price": 69.67885853853929, "storeid": "Store_2", "__ts": "2024-01-20T15:32:41.661000"}
2024-01-20 15:32:43.851 [INFO]: Received message #177 [purchase]: {"quantity": 3, "sku": "sku_5", "price": 37.138667281264155, "storeid": "Store_0", "__ts": "2024-01-20T15:32:41.744000"}
2024-01-20 15:32:43.851 [INFO]: Received message #178 [purchase]: {"quantity": 4, "sku": "sku_5", "price": 42.793267685950234, "storeid": "Store_0", "__ts": "2024-01-20T15:32:41.757000"}
2024-01-20 15:32:43.851 [INFO]: Received message #179 [purchase]: {"quantity": 5, "sku": "sku_8", "price": 50.91052356352696, "storeid": "Store_2", "__ts": "2024-01-20T15:32:41.796000"}
2024-01-20 15:32:43.851 [INFO]: Received message #180 [purchase]: {"quantity": 4, "sku": "sku_2", "price": 66.21627245415722, "storeid": "Store_2", "__ts": "2024-01-20T15:32:41.890000"}
2024-01-20 15:32:43.851 [INFO]: Received message #181 [purchase]: {"quantity": 4, "sku": "sku_8", "price": 65.2000710489714, "storeid": "Store_2", "__ts": "2024-01-20T15:32:41.981000"}
2024-01-20 15:32:43.851 [INFO]: Received message #182 [purchase]: {"quantity": 7, "sku": "sku_1", "price": 67.75588991890473, "storeid": "Store_2", "__ts": "2024-01-20T15:32:42.072000"}
2024-01-20 15:32:43.851 [INFO]: Received message #183 [purchase]: {"quantity": 3, "sku": "sku_2", "price": 38.524373074855724, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.094000"}
2024-01-20 15:32:43.851 [INFO]: Received message #184 [purchase]: {"quantity": 7, "sku": "sku_8", "price": 41.13414824029763, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.120000"}
2024-01-20 15:32:43.851 [INFO]: Received message #185 [purchase]: {"quantity": 6, "sku": "sku_1", "price": 83.65869309329382, "storeid": "Store_2", "__ts": "2024-01-20T15:32:42.149000"}
2024-01-20 15:32:43.851 [INFO]: Received message #186 [purchase]: {"quantity": 5, "sku": "sku_4", "price": 67.61138411638952, "storeid": "Store_2", "__ts": "2024-01-20T15:32:42.217000"}
2024-01-20 15:32:43.851 [INFO]: Received message #187 [purchase]: {"quantity": 5, "sku": "sku_5", "price": 85.9377660503823, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.314000"}
2024-01-20 15:32:43.851 [INFO]: Received message #188 [purchase]: {"quantity": 3, "sku": "sku_2", "price": 52.873756470025285, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.376000"}
2024-01-20 15:32:43.851 [INFO]: Received message #189 [purchase]: {"quantity": 4, "sku": "sku_5", "price": 19.98838931334801, "storeid": "Store_2", "__ts": "2024-01-20T15:32:42.431000"}
2024-01-20 15:32:43.851 [INFO]: Received message #190 [purchase]: {"quantity": 5, "sku": "sku_2", "price": 26.034123610179797, "storeid": "Store_2", "__ts": "2024-01-20T15:32:42.471000"}
2024-01-20 15:32:43.851 [INFO]: Received message #191 [purchase]: {"quantity": 1, "sku": "sku_5", "price": 25.27580646420681, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.546000"}
2024-01-20 15:32:43.851 [INFO]: Received message #192 [purchase]: {"quantity": 1, "sku": "sku_8", "price": 50.15262419131556, "storeid": "Store_1", "__ts": "2024-01-20T15:32:42.585000"}
2024-01-20 15:32:43.851 [INFO]: Received message #193 [purchase]: {"quantity": 5, "sku": "sku_8", "price": 13.620891646527568, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.660000"}
2024-01-20 15:32:43.851 [INFO]: Received message #194 [purchase]: {"quantity": 7, "sku": "sku_2", "price": 10.995696527049056, "storeid": "Store_1", "__ts": "2024-01-20T15:32:42.733000"}
2024-01-20 15:32:43.851 [INFO]: Received message #195 [purchase]: {"quantity": 5, "sku": "sku_7", "price": 20.35807145190648, "storeid": "Store_2", "__ts": "2024-01-20T15:32:42.762000"}
2024-01-20 15:32:43.851 [INFO]: Received message #196 [purchase]: {"quantity": 9, "sku": "sku_1", "price": 89.68735890007697, "storeid": "Store_1", "__ts": "2024-01-20T15:32:42.819000"}
2024-01-20 15:32:43.851 [INFO]: Received message #197 [purchase]: {"quantity": 1, "sku": "sku_0", "price": 19.4853373813775, "storeid": "Store_1", "__ts": "2024-01-20T15:32:42.849000"}
2024-01-20 15:32:43.851 [INFO]: Received message #198 [purchase]: {"quantity": 8, "sku": "sku_8", "price": 77.26858915235006, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.924000"}
2024-01-20 15:32:43.851 [INFO]: Received message #199 [purchase]: {"quantity": 5, "sku": "sku_6", "price": 72.99714235176783, "storeid": "Store_0", "__ts": "2024-01-20T15:32:42.950000"}
2024-01-20 15:32:43.852 [INFO]: Received message #200 [purchase]: {"quantity": 6, "sku": "sku_2", "price": 14.279163636407956, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.009000"}
2024-01-20 15:32:43.852 [INFO]: Adding 200 record(s) into the database...
2024-01-20 15:32:46.104 [INFO]: Exporting data to: data/database_1705764766...
2024-01-20 15:32:46.119 [INFO]: Completed!
2024-01-20 15:32:46.132 [INFO]: Received message #1 [purchase]: {"quantity": 1, "sku": "sku_0", "price": 66.8966498230822, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.035000"}
2024-01-20 15:32:46.132 [INFO]: Received message #2 [purchase]: {"quantity": 5, "sku": "sku_1", "price": 9.212307018855292, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.132000"}
2024-01-20 15:32:46.132 [INFO]: Received message #3 [purchase]: {"quantity": 4, "sku": "sku_0", "price": 53.39635278883901, "storeid": "Store_0", "__ts": "2024-01-20T15:32:43.205000"}
2024-01-20 15:32:46.132 [INFO]: Received message #4 [purchase]: {"quantity": 2, "sku": "sku_3", "price": 48.345219628537656, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.256000"}
2024-01-20 15:32:46.132 [INFO]: Received message #5 [purchase]: {"quantity": 8, "sku": "sku_5", "price": 88.26100570655228, "storeid": "Store_0", "__ts": "2024-01-20T15:32:43.285000"}
2024-01-20 15:32:46.132 [INFO]: Received message #6 [purchase]: {"quantity": 2, "sku": "sku_0", "price": 4.235323412584693, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.324000"}
2024-01-20 15:32:46.132 [INFO]: Received message #7 [purchase]: {"quantity": 8, "sku": "sku_6", "price": 43.40179139932946, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.369000"}
2024-01-20 15:32:46.132 [INFO]: Received message #8 [purchase]: {"quantity": 6, "sku": "sku_5", "price": 23.539601672007773, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.370000"}
2024-01-20 15:32:46.132 [INFO]: Received message #9 [purchase]: {"quantity": 2, "sku": "sku_6", "price": 42.82640173060379, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.433000"}
2024-01-20 15:32:46.132 [INFO]: Received message #10 [purchase]: {"quantity": 1, "sku": "sku_0", "price": 24.63279142925658, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.452000"}
2024-01-20 15:32:46.132 [INFO]: Received message #11 [purchase]: {"quantity": 7, "sku": "sku_3", "price": 94.19752730290476, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.515000"}
2024-01-20 15:32:46.132 [INFO]: Received message #12 [purchase]: {"quantity": 1, "sku": "sku_8", "price": 37.27014092906956, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.537000"}
2024-01-20 15:32:46.132 [INFO]: Received message #13 [purchase]: {"quantity": 7, "sku": "sku_8", "price": 56.89981419016357, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.572000"}
2024-01-20 15:32:46.133 [INFO]: Received message #14 [purchase]: {"quantity": 6, "sku": "sku_1", "price": 24.5257613017651, "storeid": "Store_0", "__ts": "2024-01-20T15:32:43.671000"}
2024-01-20 15:32:46.133 [INFO]: Received message #15 [purchase]: {"quantity": 2, "sku": "sku_2", "price": 73.71191654966404, "storeid": "Store_1", "__ts": "2024-01-20T15:32:43.748000"}
2024-01-20 15:32:46.133 [INFO]: Received message #16 [purchase]: {"quantity": 5, "sku": "sku_1", "price": 51.72552200414654, "storeid": "Store_0", "__ts": "2024-01-20T15:32:43.825000"}
2024-01-20 15:32:46.133 [INFO]: Received message #17 [purchase]: {"quantity": 2, "sku": "sku_6", "price": 61.83264269748485, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.827000"}
2024-01-20 15:32:46.133 [INFO]: Received message #18 [stock_trade]: {"side": "BUY", "quantity": 9, "symbol": "STK_2", "price": 17.26928451317985, "account": "Account_73", "userid": "User_69", "__ts": "2024-01-20T15:32:43.847000"}
2024-01-20 15:32:46.133 [INFO]: Received message #19 [purchase]: {"quantity": 9, "sku": "sku_6", "price": 71.43749838081376, "storeid": "Store_0", "__ts": "2024-01-20T15:32:43.860000"}
2024-01-20 15:32:46.133 [INFO]: Received message #20 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_1", "price": 24.47683902042401, "account": "Account_86", "userid": "User_07", "__ts": "2024-01-20T15:32:43.862000"}
2024-01-20 15:32:46.133 [INFO]: Received message #21 [purchase]: {"quantity": 1, "sku": "sku_2", "price": 89.72165315772794, "storeid": "Store_2", "__ts": "2024-01-20T15:32:43.919000"}
2024-01-20 15:32:46.133 [INFO]: Received message #22 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_2", "price": 12.228950282963055, "account": "Account_96", "userid": "User_76", "__ts": "2024-01-20T15:32:43.938000"}
2024-01-20 15:32:46.133 [INFO]: Received message #23 [stock_trade]: {"side": "SELL", "quantity": 2, "symbol": "STK_3", "price": 8.37875110908983, "account": "Account_16", "userid": "User_28", "__ts": "2024-01-20T15:32:43.964000"}
2024-01-20 15:32:46.133 [INFO]: Received message #24 [stock_trade]: {"side": "BUY", "quantity": 8, "symbol": "STK_7", "price": 17.352985377663508, "account": "Account_78", "userid": "User_99", "__ts": "2024-01-20T15:32:44.013000"}
2024-01-20 15:32:46.133 [INFO]: Received message #25 [purchase]: {"quantity": 7, "sku": "sku_5", "price": 49.10867703518304, "storeid": "Store_2", "__ts": "2024-01-20T15:32:44.015000"}
2024-01-20 15:32:46.133 [INFO]: Received message #26 [purchase]: {"quantity": 5, "sku": "sku_7", "price": 35.12106525730054, "storeid": "Store_1", "__ts": "2024-01-20T15:32:44.050000"}
2024-01-20 15:32:46.133 [INFO]: Received message #27 [stock_trade]: {"side": "SELL", "quantity": 7, "symbol": "STK_6", "price": 1.3238846736875818, "account": "Account_17", "userid": "User_96", "__ts": "2024-01-20T15:32:44.059000"}
2024-01-20 15:32:46.133 [INFO]: Received message #28 [stock_trade]: {"side": "BUY", "quantity": 7, "symbol": "STK_6", "price": 7.405110534621996, "account": "Account_98", "userid": "User_66", "__ts": "2024-01-20T15:32:44.080000"}
2024-01-20 15:32:46.133 [INFO]: Received message #29 [stock_trade]: {"side": "SELL", "quantity": 8, "symbol": "STK_2", "price": 9.652245577982564, "account": "Account_97", "userid": "User_66", "__ts": "2024-01-20T15:32:44.106000"}
2024-01-20 15:32:46.133 [INFO]: Received message #30 [purchase]: {"quantity": 1, "sku": "sku_0", "price": 64.4273931605159, "storeid": "Store_1", "__ts": "2024-01-20T15:32:44.115000"}
2024-01-20 15:32:46.133 [INFO]: Received message #31 [stock_trade]: {"side": "BUY", "quantity": 5, "symbol": "STK_1", "price": 15.999842569965171, "account": "Account_00", "userid": "User_32", "__ts": "2024-01-20T15:32:44.121000"}
2024-01-20 15:32:46.133 [INFO]: Received message #32 [purchase]: {"quantity": 2, "sku": "sku_8", "price": 74.55759681091965, "storeid": "Store_0", "__ts": "2024-01-20T15:32:44.148000"}
2024-01-20 15:32:46.133 [INFO]: Received message #33 [stock_trade]: {"side": "SELL", "quantity": 9, "symbol": "STK_7", "price": 6.934220473340109, "account": "Account_95", "userid": "User_23", "__ts": "2024-01-20T15:32:44.159000"}
^C2024-01-20 15:32:49.366 [WARNING]: CTRL-C pressed by user!
2024-01-20 15:32:49.366 [INFO]: Closing consumer parquet-demo-01 (parquet-demo)
2024-01-20 15:32:49.366 [INFO]: Adding 33 record(s) into the database...
2024-01-20 15:32:49.371 [INFO]: Exporting data to: data/database_1705764769...
2024-01-20 15:32:49.373 [INFO]: Completed!
```

An HTML dashboard (`analytics.html`) will be created and it should be automatically open once generated (that report will refresh itself every second):
![image](static/docs/analytics_dashboard.png)

# Stopping the demo
To stop the demo, please run `./demo_stop.sh`.
```
 ✔ Container connect                 Removed
 ✔ Container control-center          Removed
 ✔ Container schema-registry         Removed
 ✔ Container broker                  Removed
 ✔ Container zookeeper               Removed
 ✔ Network kafka_to_parquet_default  Removed 
```

# External References
Check out [Confluent's Developer portal](https://developer.confluent.io), it has free courses, documents, articles, blogs, podcasts and so many more content to get you up and running with a fully managed Apache Kafka service.

Disclaimer: I work for Confluent :wink:
