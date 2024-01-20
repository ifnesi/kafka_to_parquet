import os
import json
import time
import duckdb
import logging
import argparse
import datetime
import threading

from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

import utils

from data_analytics import run_analytics


def main(args):
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    kconfig = ConfigParser()
    kconfig.read(os.path.join(utils.FOLDER_CONFIG, args.config_filename))

    # Delete data if required
    if args.delete:
        for root, dirs, files in os.walk(utils.DUCKDB_DATA_FOLDER, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    # Start analytics thread
    threading.Thread(target=run_analytics, daemon=True).start()

    # Configure Kafka consumer
    conf_confluent = {
        "group.id": args.group_id,
        "client.id": args.client_id,
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": False,
    }
    conf_confluent.update(dict(kconfig["kafka"]))
    consumer = Consumer(conf_confluent)

    # Configure SR client
    sr_conf = dict(kconfig["schema-registry"])
    sr_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(
        sr_client,
    )

    with duckdb.connect() as conn:
        try:
            consumer.subscribe(args.topic)

            logging.info(
                f"Started consumer {conf_confluent['client.id']} ({conf_confluent['group.id']}) on topic(s): {', '.join(args.topic)}'"
            )

            records = dict()
            schema_field_names = dict()
            num_records = 0
            last_record = time.time()
            while True:
                try:
                    msg = consumer.poll(timeout=0.25)
                    if msg is not None:
                        if msg.error():
                            raise KafkaException(msg.error())
                        else:
                            topic = msg.topic()
                            # Avro deserialise the message
                            value = msg.value()
                            deserialised_record = avro_deserializer(
                                value,
                                SerializationContext(
                                    topic,
                                    MessageField.VALUE,
                                ),
                            )
                            if topic not in records:
                                records[topic] = list()
                                # Create table if needed
                                schema_id = int.from_bytes(value[1:5], "big")
                                schema_field_names[topic], table_schema = utils.generate_table_schema(json.loads(sr_client._cache.schema_id_index[schema_id].schema_str))
                                utils.create_db_table(conn, topic, table_schema)
                                utils.empty_table(conn, topic)

                            _records = list()
                            deserialised_record["__ts"] = datetime.datetime.fromtimestamp(msg.timestamp()[1] / 1000).isoformat()
                            for field in schema_field_names[topic]:
                                _records.append(deserialised_record[field])
                            records[topic].append(_records)
                            num_records += 1
                            last_record = time.time()
                            logging.info(f"Received message #{num_records} [{topic}]: {json.dumps(deserialised_record)}")

                    # Dump to DB and save as Parquet
                    if (num_records >= args.dump_records) or (num_records > 0 and (time.time() - last_record) > args.dump_timeout):
                        utils.insert_and_export(conn, utils.DUCKDB_DATA_FOLDER, num_records, schema_field_names, records)
                        consumer.commit(asynchronous=False)
                        num_records = 0
                        last_record = time.time()
                        for topic in records.keys():
                            records[topic] = list()

                except Exception as err:
                    logging.error(err)

        except KeyboardInterrupt:
            logging.warning("CTRL-C pressed by user!")

        finally:
            logging.info(
                f"Closing consumer {conf_confluent['client.id']} ({conf_confluent['group.id']})"
            )
            if (num_records > 0):
                utils.insert_and_export(conn, utils.DUCKDB_DATA_FOLDER, num_records, schema_field_names, records)
                consumer.commit(asynchronous=False)
            consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python Kafka to Parquet (Avro serialised topics)")
    OFFSET_RESET = [
        "earliest",
        "latest",
    ]
    parser.add_argument(
        "--topics",
        help="List of topic names (default is 'stock_trade')",
        dest="topic",
        type=str,
        nargs="+",
        default=["stock_trade"],
    )
    parser.add_argument(
        "--offset-reset",
        dest="offset_reset",
        help=f"Set auto.offset.reset (default: {OFFSET_RESET[0]})",
        type=str,
        default=OFFSET_RESET[0],
        choices=OFFSET_RESET,
    )
    parser.add_argument(
        "--config-filename",
        dest="config_filename",
        type=str,
        help=f"Select config filename for additional configuration, such as credentials (files must be inside the folder {utils.FOLDER_CONFIG}/)",
        default="localhost.ini",
    )
    parser.add_argument(
        "--kafka-section",
        dest="kafka_section",
        type=str,
        help="Section in the config file related to the Kafka cluster (default is 'kafka')",
        default="kafka",
    )
    parser.add_argument(
        "--sr-section",
        dest="sr_section",
        type=str,
        help="Section in the config file related to the Schema Registry (default is 'schema-registry')",
        default="schema-registry",
    )
    parser.add_argument(
        "--group-id",
        dest="group_id",
        type=str,
        help="Consumer's Group ID (default is 'parquet-demo')",
        default="parquet-demo",
    )
    parser.add_argument(
        "--client-id",
        dest="client_id",
        type=str,
        help="Consumer's Client ID (default is 'parquet-demo-01')",
        default="parquet-demo-01",
    )
    parser.add_argument(
        "--dump-records",
        dest="dump_records",
        type=int,
        help="Number os records to create a new Parquet file",
        default=200,
    )
    parser.add_argument(
        "--dump-timeout",
        dest="dump_timeout",
        type=int,
        help="Record timeout to create a new Parquet file (seconds)",
        default=60,
    )
    parser.add_argument(
        "-d",
        "--delete",
        dest="delete",
        help="Delete current data",
        action="store_true",
    )

    main(parser.parse_args())
