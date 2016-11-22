import hashlib
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
        for msg in self.rfile.readlines():
            value = msg.strip()
            key = self.genkey(value)
            self.server.producer.send(self.server.topic, key=key, value=value)
            print value 
    @staticmethod
    def genkey(value):
        """Returns a str representation of a hash for the provided value."""
        return hashlib.sha224(value).hexdigest()


if __name__ == "__main__":

    listenip = environ["listenip"] if "listenip" in environ else "0.0.0.0"
    listenport = int(environ["listenport"]) if "listenport" in environ else 1543
    topic = environ["topic"] if "topic" in environ else "mytopic"
    bootstrap_servers = environ["bootstrap_servers"] if "bootstrap_servers" in environ else "localhost:9092"

    producer_conf = {
        "bootstrap_servers": bootstrap_servers,
        "key_serializer": str.encode,
        }

    server = socketserver.TCPServer((listenip, listenport), KafkaProducerTCPHandler)
    server.producer = KafkaProducer(**producer_conf)
    server.topic = topic
    server.serve_forever()
