from confluent_kafka import Producer
import json
import logging
import random
import sys
import time

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


producer: Producer = Producer(
    {
        "bootstrap.servers": config.get("bootstrapServers"),
        "queue.buffering.max.messages": 10000000,
    }
)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


# Trigger any available delivery report callbacks from previous produce() calls
producer.poll(0)

timestamp = time.time()
# Constuct message to send
message = {
    "name": "your_name_here",
    "throughput": "your_answer_here",
    "timestamp": {"seconds": int(timestamp), "micros": int(timestamp % 1 * 10**6)},
}

# Asynchronously produce a message. The delivery report callback will
# be triggered from the call to poll() above, or flush() below, when the
# message has been successfully delivered or failed permanently.
producer.produce(
    config.get("topic"),
    json.dumps(message).encode("utf-8"),
    callback=delivery_report,
)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
producer.flush()
