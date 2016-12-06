import hashlib
import logging
import socketserver

from os import environ
from kafka import KafkaProducer


class KafkaProducerTCPHandler(socketserver.StreamRequestHandler):
    """
    TCPHandler which sends received data using a configured producer.
    A key is produced for each message by hashing the message.
    You must explicitly set a producer and a topic as a property of the server.

    For example:

    server = socketserver.TCPServer((HOST, PORT), MyTCPHandler)
    server.producer = KafkaProducer(...)
    server.topic = "mytopic"
    server.serve_forever()
    """

    def handle(self):
        for msg in iter(self.rfile.readline, ''.encode()):

            value = msg.decode().strip()
            if value == '':
                break

            key = self.genkey(value)
            logging.info("Sending: {}".format(value))

            try:
                self.server.producer.send(self.server.topic, key=key, value=value)
            except:
                logging.exception("Failed to send {}".format(value))

    @staticmethod
    def genkey(value):
        """Returns a str representation of a hash for the provided value."""
        return hashlib.sha224(value.encode()).hexdigest()

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

if __name__ == "__main__":

    listenip = environ["listenip"] if "listenip" in environ else "0.0.0.0"
    listenport = int(environ["listenport"]) if "listenport" in environ else 1543
    topic = environ["topic"] if "topic" in environ else "mytopic"
    bootstrap_servers = environ["bootstrap_servers"] if "bootstrap_servers" in environ else "localhost:9092"
    loglevel = environ["loglevel"] if "loglevel" in environ else "INFO"

    level = getattr(logging, loglevel.upper())
    logging.basicConfig(level=level)

    producer_conf = {
        "bootstrap_servers": bootstrap_servers,
        "key_serializer": str.encode,
        "value_serializer": str.encode,
        "retries": 3,
        "linger_ms": 100,
        }

    server = ThreadedTCPServer((listenip, listenport), KafkaProducerTCPHandler)
    server.producer = KafkaProducer(**producer_conf)
    server.topic = topic

    server.serve_forever()

