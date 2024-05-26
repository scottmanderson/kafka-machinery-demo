from confluent_kafka import Consumer
import json
import logging
import sys

config_file = None
config = None

try:
    config_file = open(sys.argv[1])
except IndexError as e:
    logging.error(
        "Appears no argument was provided to program; program expects a json config filename as argument"
    )
    exit()
except FileNotFoundError as e:
    logging.error(f"Specified Config File {sys.argv[1]} not found")
    exit()

try:
    config: dict[str, str] = json.load(config_file)
except json.JSONDecodeError:
    logging.error("Could not parse JSON config file, exiting...")
    exit()

consumer = Consumer(
    {
        "bootstrap.servers": config.get("bootstrapServers"),
        "group.id": config.get("consumerGroupId"),
        "auto.offset.reset": config.get("autoOffsetReset"),
    }
)

consumer.subscribe([config.get("topic")])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print("Received message: {}".format(msg.value().decode("utf-8")))

consumer.close()
