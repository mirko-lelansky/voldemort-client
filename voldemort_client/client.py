import logging
import random
import re
import socket
import struct
import time
from voldemort_client.exception import VoldemortException
from voldemort_client.protocol import voldemort_client_v2_pb2 as protocol
from voldemort_client import serialization
from xml.dom import minidom

class VoldemortClient:
    """
    This class manages the connection to a voldemort cluster and provides
    the accessing methods.
    """

    def __init__(self, store_name, bootstrap_urls, conflict_resolver = None, reconnect_interval = 500):
        """
        This is the constructor method of the class.
        """
        self._connection = None
        self._request_count = 0
        self._conflict_resolver = conflict_resolver
        self._store_name = store_name
        self._bootstrap_urls = bootstrap_urls
        self._nodes = None
        self._store = None
        self._nodes, self._store = self._bootstrap(self._bootstrap_urls, self._store_name)
        if not self._store:
            raise VoldemortException("Cannot find store [%s] at %s." % (self._store_name, self._bootstrap_urls))
        self._node_id = random.randint(0, len(self._nodes) - 1)
        self._reconnect_interval = reconnect_interval
        self._node_id, self._connection = self._reconnect()
        self._key_serializer = self._store.key_serializer
        self._value_serializer = self._store.value_serializer
        self._open = True

    ## A basic request wrapper, that handles reconnection logic and failures
    def _execute_request(self, fun, args):
        assert self._open, 'Store has been closed.'
        self._maybe_reconnect()

        failures = 0
        num_nodes = len(self._nodes)
        while failures < num_nodes:
            try:
                return fun(args)
            except socket.error:
                logging.warn('Error while performing ' + fun.__name__ + ' on node ' + str(self._node_id) + ': ' + "message")
                self._node_id, self._connection = self._reconnect()
                failures += 1
        raise VoldemortException('All nodes are down, ' + fun.__name__ + ' failed.')

    ## Increment the version for a vector clock
    def _increment(self, clock):
        new_clock = protocol.VectorClock()
        new_clock.MergeFrom(clock)

        # See if we already have a version for this guy, if so increment it
        for entry in new_clock.entries:
            if entry.node_id == self.node_id:
                entry.version += 1
                return new_clock

        # Otherwise add a version
        entry = new_clock.entries.add()
        entry.node_id = self.node_id
        entry.version = 1
        new_clock.timestamp = int(time.time() * 1000)

        return new_clock

    def _put(self, key, value, version):
        req = protocol.VoldemortRequest()
        req.should_route = True
        req.store = self.store_name
        req.type = protocol.PUT
        req.put.key = key
        req.put.versioned.value = value
        req.put.versioned.version.MergeFrom(version)

        # send request
        self._send_request(self.connection, req.SerializeToString())

        # read and parse response
        resp_str = self._receive_response(self.connection)
        resp = protocol.PutResponse()
        resp.ParseFromString(resp_str)
        self._check_error(resp)
        return self._increment(version)

    def put(self, key, value, version = None):
        """Execute a put request using the given key and value. If no version is specified a get(key) request
           will be done to get the current version. The updated version is returned."""

        raw_key = self.key_serializer.writes(key)
        raw_value = self.value_serializer.writes(value)

        # if we don't have a version, fetch one
        if not version:
            version = self._fetch_version(key)
        return self._execute_request(self._put, [raw_key, raw_value, version])

    ## Check if the the number of requests made on this connection is greater than the reconnect interval.
    ## If so reconnect to a random node in the cluster. No attempt is made at preventing the reconnecting
    ## from going back to the same node
    def _maybe_reconnect(self):
        if self._request_count >= self._reconnect_interval:
            logging.debug('Completed ' + str(self._request_count) + ' requests using this connection, reconnecting...')
            self._node_id, self._connection = self._reconnect()

    def _get(self, key):
        return self._get_with_connection(self._connection, self._store_name, key, True)

    def get(self, key):
        """
        This method fetch the value from the cluster which is under the given
        key stored.
        """
        raw_key = self._key_serializer.writes(key)
        return [(self._value_serializer.reads(value), version)
                for value, version in self._execute_request(self._get, [raw_key])]

    def set(self, key, value):
        """
        This method stores the given key value pair on the cluster.
        """
        self.put(key, value)

    def close(self):
        """
        This method close the connection to the cluster and clean the resources.
        """
        self._close_connection(self._connection)

    def _bootstrap(self, bootstrap_urls, store_name):
        """
        This method bootstraps the client with the metadata from the cluster.
        """
        random.shuffle(bootstrap_urls)
        for host, port in bootstrap_urls:
            logging.debug("Attempt to bootstrap metadata from %s:%d." % (host, port))
            connection = None
            nodes = None
            store = None
            try:
                connection = self._open_connection(host, port)
                cluster_xmls = self._get_with_connection(connection, "metadata", "cluster.xml", should_route = False)
                if len(cluster_xmls) != 1:
                    raise VoldemortException("Only one cluster_xml. But found %d." % len(cluster_xmls))
                nodes = Node.parse_cluster_xml(cluster_xmls[0][0])
                stores_xml = self._get_with_connection(connection, "metadata", "stores.xml", should_route = False)
                if len(stores_xml) != 1:
                    raise VoldemortException("No cluster xmls found.")
                store = Store.parse_store_xml(stores_xml[0][0], store_name)
                print(store)
            except socket.error as e:
                logging.warn("Metadata fetch form host %s failed: %s." % (host, e))
            finally:
                self._close_connection(connection)
            return nodes, store
        raise VoldemortException("All bootstrap attemps failed.")

    def _open_connection(self, host, port, protocol="pb0"):
        """
        This method established a connection to the voldemort cluster.
        """
        logging.debug("Attempt to connect to %s:%d" % (host, port))
        connection = socket.socket()
        connection.connect((host, port))
        logging.debug("Connection succeeded, negotiating protocol.")
        connection.send(protocol.encode())
        resp = connection.recv(2).decode()
        if resp != "ok":
            raise VoldemortException("The server didn't understand the protocol %s." % (protocol))
        logging.debug("Negotiating protocuall succeeded.")
        return connection

    def _close_connection(self, connection):
        """
        This method closed the open connection.
        """
        if connection:
            connection.close()
            self._open = False

    def _get_with_connection(self, connection, store_name, key, should_route = False):
        """
        Execute a get request to the given store. Returns a (value, version) pair.
        """
        req = protocol.VoldemortRequest()
        req.should_route = should_route
        req.store = store_name
        req.type = protocol.GET

        if isinstance(key, str):
            req.get.key = key.encode()
        else:
            req.get.key = ', '.join(key).encode()

        self._send_request(connection, req.SerializeToString())

        resp_str = self._receive_response(connection)
        resp = protocol.GetResponse()
        resp.ParseFromString(resp_str)

        self._check_error(resp)

        return self._extract_versions(resp.versioned)

    def _send_request(self, connection, req_bytes):
        connection.send(struct.pack(">i", len(req_bytes)) + req_bytes)
        self._request_count += 1

    def _receive_response(self, connection):
        size_bytes = connection.recv(4)
        if not size_bytes:
            raise VoldemortException("Connection closed.")
        size = struct.unpack(">i", size_bytes)[0]
        
        bytes_read = 0
        data = []

        while size and bytes_read < size:
            chunk = connection.recv(size - bytes_read)
            bytes_read += len(chunk)
            data.append(chunk)

        return b"".join(data)

    def _reconnect(self):
        num_nodes = len(self._nodes)
        attempts = 0
        new_node_id = self._node_id
        self._close_connection(self._connection)
        while attempts < num_nodes:
            new_node_id = (new_node_id + 1) % num_nodes
            new_node = self._nodes[new_node_id]
            connection = None
            try:
                connection = self._open_connection(new_node._host, new_node._socket_port)
                self._request_count = 0
                return new_node_id, connection
            except socket.error as e:
                logging.warn('Error connecting to node ' + str(new_node_id) + ': ' + "msg")
                attempts += 1

        # If we get here all nodes have failed us, explode
        raise VoldemortException('Connections to all nodes failed.')

    def _check_error(self, resp):
        if resp.error and resp.error.error_code != 0:
            raise VoldemortException(resp.error.error_message, resp.error.error_code)

    def _extract_versions(self, pb_versioneds):
        versions = []
        for versioned in pb_versioneds:
            versions.append((versioned.value, versioned.version))
        return self._resolve_conflicts(versions)

    def _resolve_conflicts(self, versions):
        if self._conflict_resolver and versions:
            return self._conflict_resolver(versions)
        else:
            return versions

def _child_text(element, name, required = True, default = None):
    if default:
        required = False

    child = _child(element, name, required = required)
    if not child:
        return default

    return _extract_text(child)

def _child(element, name, required=True):
    children = [child for child in element.childNodes
            if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName == name]
    if not children:
        if required:
            raise VoldemortException("No child '%s' for element '%s'." % (name, element.nodeName))
        else:
            return None

    if len(children) > 1:
        raise VoldemortException("Multiple children '%s' for element '%s'." % (name, element.nodeName))
    return children[0]

def _extract_text(element):
    if element.nodeType == minidom.Node.TEXT_NODE:
        return element.data
    elif element.nodeType == minidom.Node.ELEMENT_NODE:
        text = ""
        for child in element.childNodes:
            text += _extract_text(child)
        return text

class Node:
    """
    """

    def __init__(self, id, host, socket_port, http_port, partitions, is_available = True, last_contact = None):
        self._id = id
        self._host = host
        self._socket_port = socket_port
        self._partitions = partitions
        self._is_available = is_available
        if not last_contact:
            self.last_contact = time.clock()

    def __str__(self):
        return ""

    @classmethod
    def parse_cluster_xml(cls, xml):
        doc = minidom.parseString(xml)
        nodes = {}
        for curr in doc.getElementsByTagName("server"):
            id = int(_child_text(curr, "id"))
            host = _child_text(curr, "host")
            http_port = int(_child_text(curr, "http-port"))
            socket_port = int(_child_text(curr, "socket-port"))
            partitions_str = _child_text(curr, "partitions")
            partitions = [int(p) for p in re.split('[\s,]+', partitions_str)]
            nodes[id] = cls(id, host, socket_port, http_port, partitions)
        return nodes

def _int_or_none(s):
    if s is None:
        return s
    return int(s)

class Store:
    """
    """

    def __init__(self, store_node):
        """
        """
        self.name = _child_text(store_node, "name")
        self.persistence = _child_text(store_node, "persistence")
        self.routing = _child_text(store_node, "routing")
        self.routing_strategy = _child_text(store_node, "routing-strategy", default="consistent-routing")
        self.replication_factor = int(_child_text(store_node, "replication-factor"))
        self.required_reads = int(_child_text(store_node, "required-reads"))
        self.preferred_reads = _int_or_none(_child_text(store_node, "preferred-reads", required=False))
        self.required_writes = int(_child_text(store_node, "required-writes"))
        self.preferred_writes = _int_or_none(_child_text(store_node, "preferred-writes", required=False))

        key_serializer_node = _child(store_node, "key-serializer")
        try:
            self.key_serializer_type = _child_text(key_serializer_node, "type")
            self.key_serializer = self._create_serializer(self.key_serializer_type, key_serializer_node)
        except serialization.SerializationException as e:
            logging.warn("Error while creating key serializer for store [%s]: %s" % (self.name, e))
            self.key_serializer_type = "invalid"
            self.key_serializer = serialization.UnimplementedSerializer("invalid")

        value_serializer_node = _child(store_node, "value-serializer")
        try:
            self.value_serializer_type = _child_text(value_serializer_node, "type")
            self.value_serializer = self._create_serializer(self.value_serializer_type, value_serializer_node)
        except serialization.SerializationException as e:
            logging.warn("Error while creating value serializer for store [%s]: %s" % (self.name, e))
            self.value_serializer_type = "invalid"
            self.value_serializer = serialization.UnimplementedSerializer("invalid")

    def _create_serializer(self, serializer_type, serializer_node):
        if serializer_type not in serialization.SERIALIZER_CLASSES:
            return serialization.UnimplementedSerializer(serializer_type)

        return serialization.SERIALIZER_CLASSES[serializer_type].create_from_xml(serializer_node)

    @classmethod
    def parse_store_xml(cls, xml, store_name):
        doc = minidom.parseString(xml)
        store_nodes = doc.getElementsByTagName("store")
        for store_node in store_nodes:
            name = _child_text(store_node, "name")
            if name == store_name:
                return Store(store_node)
        return None

