import re
import time
from voldemort_client import helper
from voldemort_client import serialization
from xml.dom import minidom

class Node:
    """
    This class stands for one server of the cluster.
    """

    def __init__(self, id, host, http_port, socket_port, partitions, is_available = True, last_contact = None):
        """
        This is the initialisation method of the class.

        :param id:
        :type id: int
        :param host:
        :type host: str
        :param http_port:
        :type http_port: int
        :param socket_port:
        :type socket_port: int
        :param partitions:
        :type partitions:
        :param is_available:
        :type is_available: bool
        :param last_contact:
        :type last_contact: time.clock
        """
        self.id = id
        self.host = host
        self.http_port = http_port
        self.socket_port = socket_port
        self.partitions = partitions
        self.is_available = is_available
        self.last_contact = last_contact

    @property
    def id(self):
        """
        This is the getter for the id.
        """
        return self._id

    @id.setter
    def id(self, id):
        """
        This is the setter for the id.
        """
        self._id = id

    @property
    def host(self):
        """
        This is the getter for the host.
        """
        return self._host

    @host.setter
    def host(self, host):
        """
        This is the setter for the host.
        """
        self._host = host

    @property
    def http_port(self):
        """
        This is the getter for the http port.
        """
        return self._http_port

    @http_port.setter
    def http_port(self, http_port):
        """
        This is the setter for the http port.
        """
        self._http_port = http_port

    @property
    def socket_port(self):
        """
        This is the getter for the socket port.
        """
        return self._socket_port

    @socket_port.setter
    def socket_port(self, socket_port):
        """
        This is the setter for the socket port.
        """
        self._socket_port = socket_port

    @property
    def partitions(self):
        """
        This is the getter for the partitions.
        """
        return self._partitions

    @partitions.setter
    def partitions(self, partitions):
        """
        This is the setter for the partitions.
        """
        self._partitions = partitions

    @property
    def is_available(self):
        """
        This is the getter for the is_available flag.
        """
        return self._is_available

    @is_available.setter
    def is_available(self, is_available):
        """
        This is the setter for the is_available flag.
        """
        self._is_available = is_available

    @property
    def last_contact(self):
        """
        This is the getter for the last contact.
        """
        return self._last_contact

    @last_contact.setter
    def last_contact(self, last_contact):
        """
        This is the setter for the last contact.
        """
        if last_contact is None:
            self._last_contact = time.clock()
        else:
            self._last_contact = last_contact

    @classmethod
    def parse_cluster_xml(cls, xml_content):
        """
        This method parsers the xml string from the cluster.xml and
        initializes the nodes.

        :param cls: the Node class
        :type cls: the class definition
        :param xml_content: the xml content
        :type xml_content: str
        """
        doc = minidom.parseString(xml_content)
        nodes = {}
        for curr in doc.getElementsByTagName("server"):
            id = int(helper.extract_childnode_text(curr, "id"))
            host = helper.extract_childnode_text(curr, "host")
            http_port = int(helper.extract_childnode_text(curr, "http-port"))
            socket_port = int(helper.extract_childnode_text(curr, "socket-port"))
            partitions_str = helper.extract_childnode_text(curr, "partitions")
            partitions = [int(p) for p in re.split('[\s,]+', partitions_str)]
            nodes[id] = cls(id, host, http_port, socket_port, partitions)
        return nodes

class Store:
    """
    This class stands for one store of the voldemort cluster.
    """

    def __init__(self, name, persistence, routing, routing_stategy, replication_factor, required_reads, preferred_reads, required_writes, preferred_writes, key_serializer, value_serializer):
        """
        This is the initialisation method of the class.
        """
        self.name = name
        self.persistence = persistence
        self.routing = routing
        self.routing_stategy = routing_stategy
        self.replication_factor = replication_factor
        self.required_reads = required_reads
        self.preferred_reads = preferred_reads
        self.required_writes = required_writes
        self.preferred_writes = preferred_writes
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    @property
    def name(self):
        """
        This is the getter of the name.
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        This is the setter of the name.
        """
        self._name = name

    @property
    def persistence(self):
        """
        This is the getter of the persistence.
        """
        return self._persistence

    @persistence.setter
    def persistence(self, persistence):
        """
        This is the setter of the persistence.
        """
        self._persistence = persistence

    @property
    def routing(self):
        """
        This is the getter of the routing.
        """
        return self._routing

    @routing.setter
    def routing(self, routing):
        """
        This is the setter of the routing.
        """
        self._routing = routing

    @property
    def routing_stategy(self):
        """
        This is the getter of the routing strategy.
        """
        return self._routing_strategy

    @routing_stategy.setter
    def routing_stategy(self, routing_strategy):
        """
        This is the setter of the routing strategy.
        """
        self._routing_strategy = routing_strategy

    @property
    def replication_factor(self):
        """
        This is the getter of the replication factor.
        """
        return self._replication_factor

    @replication_factor.setter
    def replication_factor(self, replication_factor):
        """
        This is the setter of the replication factor.
        """
        self._replication_factor = replication_factor

    @property
    def required_reads(self):
        """
        This is the setter of the required reads.
        """
        return self._required_reads

    @required_reads.setter
    def required_reads(self, required_reads):
        """
        This is the setter of the required reads.
        """
        self._required_reads = required_reads

    @property
    def preferred_reads(self):
        """
        This is the getter of the preferred reads.
        """
        return self._preferred_reads

    @preferred_reads.setter
    def preferred_reads(self, preferred_reads):
        """
        This is the setter of the preferred reads.
        """
        self._preferred_reads = preferred_reads

    @property
    def required_writes(self):
        """
        This is the getter of the required writes.
        """
        return self._required_writes

    @required_writes.setter
    def required_writes(self, required_writes):
        """
        This is the setter of the required writes.
        """
        self._required_writes = required_writes

    @property
    def preferred_writes(self):
        """
        This is the getter of the preferred writes.
        """
        return self._preferred_writes

    @preferred_writes.setter
    def preferred_writes(self, preferred_writes):
        """
        This is the setter of the prefered writes.
        """
        self._preferred_writes = preferred_writes

    @property
    def key_serializer(self):
        """
        This is the getter of the key serializer.
        """
        return self._key_serializer

    @key_serializer.setter
    def key_serializer(self, key_serializer):
        """
        This is the setter of the key serializer
        """
        self._key_serializer = key_serializer

    @property
    def value_serializer(self):
        """
        This is the getter of the value serializer.
        """
        return self._value_serializer

    @value_serializer.setter
    def value_serializer(self, value_serializer):
        """
        This is the setter of the value serializer.
        """
        self._value_serializer = value_serializer

    @classmethod
    def parse_store_xml(cls, xml_content, store_name):
        """
        This method parses the xml string of from the store.xml and
        initializes the store.

        :param cls: the Store class
        :type cls: the class definition
        :param xml_content: the content of the xml
        :type xml_content: str
        :type store_name: the name of the used store
        :type store_name: str
        """
        doc = minidom.parseString(xml_content)
        store_nodes = doc.getElementsByTagName("store")
        for store_node in store_nodes:
            name = helper.extract_childnode_text(store_node, "name")
            if name == store_name:
                persistence = helper.extract_childnode_text(store_node, "persistence")
                routing = helper.extract_childnode_text(store_node, "routing")
                routing_strategy = helper.extract_childnode_text(store_node, "routing-strategy", default="consistent-routing")
                replication_factor = int(helper.extract_childnode_text(store_node, "replication-factor"))
                required_reads = int(helper.extract_childnode_text(store_node, "required-reads"))
                preferred_reads = helper.int_or_none(helper.extract_childnode_text(store_node, "preferred-reads", required = False))
                required_writes = int(helper.extract_childnode_text(store_node, "required-writes"))
                preferred_writes = helper.int_or_none(helper.extract_childnode_text(store_node, "preferred-writes", required = False))
                key_serializer_node = helper.get_child(store_node, "key-serializer")
                key_serializer_type = helper.extract_childnode_text(key_serializer_node, "type")
                key_serializer = serialization.build_serializer(key_serializer_type, key_serializer_node)
                value_serializer_node = helper.get_child(store_node, "value-serializer")
                value_serializer_type = helper.extract_childnode_text(value_serializer_node, "type")
                value_serializer = serialization.build_serializer(value_serializer_type, value_serializer_node)
                return cls(name, persistence, routing, routing_strategy, replication_factor, required_reads, preferred_reads, required_writes, preferred_writes, key_serializer, value_serializer)
        return None
