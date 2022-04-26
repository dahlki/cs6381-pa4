from threading import Lock

from kazoo.client import KazooClient


class MetaData:
    def __init__(self, role, zk):
        self.role = role
        self.zk: KazooClient = zk

        self.topics = {}

        self.lock = Lock()

    def listen_topic(self, topic):
        print(f"META topic to register: {topic}")

        if self.zk.exists(f"publisher/{topic}"):
            print(f"{topic} node exists")
            address, *stat = self.zk.get(f"publisher/{topic}", self.topic_callback)
            print(f"META: {address.decode()}")
            self.topics[topic] = address
        print(self.topics)

    def topic_callback(self, data):
        print("\n********in print topic_callback: {}**********\n".format(data))

    def get_topic_address(self, topic):
        print(f"getting {topic} from metadata")

        if self.zk.exists(f"publisher/{topic}"):
            address, *stat = self.zk.get(f"publisher/{topic}", self.topic_callback)
            return address
        return None
