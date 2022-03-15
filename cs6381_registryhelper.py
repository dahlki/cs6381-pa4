import json
import cs6381_constants as constants


class RegistryHelper:
    def __init__(self, kad_client):
        self.kademlia_client = kad_client

    def set(self, key, value):
        self.kademlia_client.set(key, value)

    def get(self, key):
        value = self.kademlia_client.get(key)
        # print("helper got {} for key {}:".format(value, key))
        return value

    def get_value_list(self, key):
        value_list = self.get(key)
        if value_list is not None:
            return json.loads(value_list)
        return None

    def set_broker_ip(self, ip):
        print("setting broker ip: {}".format(ip))
        return self.set(constants.BROKER_IP, ip)

    def get_broker_ip(self):
        return self.get(constants.BROKER_IP)

    def set_broker_port(self, port):
        return self.set(constants.BROKER_PORT, port)

    def get_broker_port(self):
        return self.get(constants.BROKER_IP)

    def set_registry_node(self, node_ip):
        self.set_value_to_list(constants.REGISTRY_NODES, node_ip)

    def get_registry_nodes(self):
        return self.get(constants.REGISTRY_NODES)

    def set_topic_index(self, entry):
        index = self.get_topic_index()
        print("INDEX:", index)
        if index is not None:
            if not(entry in index):
                index = json.loads(index)
                index.append(entry)
                self.set("index", json.dumps(index))
                return True
        else:
            index = [entry]
            self.set("index", json.dumps(index))
            return True
        return False

    def get_topic_index(self):
        return self.kademlia_client.get("index")

    def is_topic_indexed(self, entry):
        index = self.get_topic_index()
        return entry in index

    def delete_from_index(self, entry):
        index = self.get_topic_index()
        if entry in index:
            index.remove(entry)
            self.set("index", json.dumps(index))

    def set_value_to_list(self, key, value):
        value_list = self.get_value_list(key)
        if value_list is not None:
            if not(value in value_list):
                value_list.append(value)
                self.set(key, json.dumps(value_list))
                return True
        else:
            value_list = [value]
            self.set(key, json.dumps(value_list))
            return True
        # return False if value is already in list for key
        return False

    def serialize(self, obj, key, value, single_value):
        if single_value:
            obj[key] = value
            return json.dumps(obj)

        if not obj or not(key in obj):
            obj[key] = [value]
        else:
            values = obj[key]
            if not(value in values):
                values.append(value)
        return json.dumps(obj)

    def get_registry(self):
        registry = self.kademlia_client.get("registry")
        print("registry from kademlia!!!!:", registry)
        if registry is not None:
            return json.loads(registry)
        return {}

    def set_registry(self, topic, connection, single_value=False):
        current_registry = self.get_registry()
        print("CURRENT_REGISTRY", current_registry)

        registry = self.serialize(current_registry, topic, connection, single_value)
        self.kademlia_client.set("registry", registry)

    def discover(self, topic):
        return self.get(topic)