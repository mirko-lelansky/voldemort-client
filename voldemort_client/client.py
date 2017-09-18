import logging
import random
from voldemort_client.connection import SocketConnector
from voldemort_client.exception import VoldemortException, ConnectionException
from voldemort_client.model import Node, Store
from voldemort_client.protocol import voldemort_client_v2_pb2 as protocol
from voldemort_client import serialization

class VoldemortClient:
    """
    This class manages the connection to a voldemort cluster and provides
    the accessing methods.
    """

    def __init__(self, store_name, bootstrap_urls, conflict_resolver = None):
        """
        This is the constructor method of the class.
        """
        self._connection = None
        self._conflict_resolver = conflict_resolver
        self._store_name = store_name
        self.bootstrap_urls = bootstrap_urls
        self._nodes = None
        self._store = None
        self._bootstrap()
        self._reconnect()
        
    def get(self, key):
        """
        This method returns the value, versions pairs from the server.
        
        :param key: the key to fetch
        :type key: str
        """
        raw_key = self._store.key_serializer.write(key)
        versions = self._connection.execute_get_request(raw_key.encode(), self._store_name, True)
        return [(self._store.value_serializer.read(value), version) for value, version in versions]

    def set(self):
        """
        """

    def close(self):
        """
        This method close the connection to the cluster and clean the resources.
        """
        if self._connection:
            self._connection.close_connection()
            self._connection = None

    def _bootstrap(self):
        """
        This method bootstraps the client with the metadata from the cluster.
        """
        random.shuffle(self._bootstrap_urls)
        for host, port in self._bootstrap_urls:
            logging.debug("Attempt to bootstrap metadata from %s:%d." % (host, port))
            conn = None
            try:
                conn = SocketConnector(self._conflict_resolver)
                conn.open_connection(host, port)
                cluster_xmls = conn.execute_get_request("cluster.xml".encode(), "metadata", should_route = False)
                if len(cluster_xmls) != 1:
                    raise VoldemortException("Only one cluster_xml. But found %d." % len(cluster_xmls))
                self._nodes = Node.parse_cluster_xml(cluster_xmls[0][0])
                stores_xml = conn.execute_get_request("stores.xml".encode(), "metadata", should_route = False)
                if len(stores_xml) != 1:
                    raise VoldemortException("No stores xmls found.")
                self._store = Store.parse_store_xml(stores_xml[0][0], self.store_name)
            except ConnectionException as e:
                logging.warn("Metadata fetch form host %s:%d failed." % (host, port))
            finally:
                conn.close_connection()
            if not self._nodes or not self._store:
                raise VoldemortException("All bootstrap attemps failed.")
            
    def _reconnect(self):
        num_nodes = len(self._nodes)
        attempts = 0
        self.close()
        while attempts < num_nodes:
            node_id = (node_id + 1) % num_nodes
            new_node = self._nodes[node_id]
            conn = None
            try:
                conn = SocketConnector(self._conflict_resolver)
                conn.open_connection(new_node.host, new_node.socket_port)
                self._connection = conn
                break
            except ConnectionException as e:
                logging.warn("Error connection to node %s:%d." %(host, port))
                attempts += 1
        if not self._connection:
            raise VoldemortException("Connections to all nodes failed.")
