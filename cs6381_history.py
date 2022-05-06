from collections import deque


class History:
    def __init__(self):
        self.address = None
        self.history = {}
        self.send_history = False

    def register(self, address, history_max, topic, value):
        self.address = address
        if not (self.address in self.history):
            self.history[self.address] = {}
        if topic in self.history[self.address]:
            self.history[self.address][topic].append(value)
        else:
            self.history[self.address][topic] = deque(maxlen=history_max)
            self.history[self.address][topic].append(value)

    def get_history(self, pub_address, topic):
        # print(f"getting history of pub {pub_address} for topic {topic}")
        if self.history.get(pub_address) and self.history.get(pub_address).get(topic):
            return list(self.history.get(pub_address).get(topic))
        else:
            return []

    def send_history(self):
        self.send_history = True


