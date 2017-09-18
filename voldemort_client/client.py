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
        self.connection = None
        self.conflict_resolver = conflict_resolver
        self.store_name = store_name
        self.bootstrap_urls = bootstrap_urls
        self.nodes = None
        self.store = None
        self._bootstrap()
        self.connection = self._reconnect()
        
    def get(self):
        """
        """

    def set(self):
        """
        """

    def close(self):
        """
        This method close the connection to the cluster and clean the resources.
        """
        if self.connection:
            self.connection._close_connection()
            self.connection = None

    def _bootstrap(self):
        """
        This method bootstraps the client with the metadata from the cluster.
        """
        random.shuffle(self.bootstrap_urls)
        for host, port in self.bootstrap_urls:
            logging.debug("Attempt to bootstrap metadata from %s:%d." % (host, port))
            connection = None
            try:
                connection = SocketConnector(self.conflict_resolver)
                connection.open_connection(host, port)
                cluster_xmls = connection.execute_get_request("cluster.xml".encode(), "metadata", should_route = False)
                if len(cluster_xmls) != 1:
                    raise VoldemortException("Only one cluster_xml. But found %d." % len(cluster_xmls))
                self.nodes = Node.parse_cluster_xml(cluster_xmls[0][0])
                stores_xml = connection.execute_get_request("stores.xml".encode(), "metadata", should_route = False)
                if len(stores_xml) != 1:
                    raise VoldemortException("No stores xmls found.")
                self.store = Store.parse_store_xml(stores_xml[0][0], self.store_name)
            except ConnectionException as e:
                logging.warn("Metadata fetch form host %s:%d failed." % (host, port))
            finally:
                connection.close_connection()
            if not self.nodes or not self.store:
                raise VoldemortException("All bootstrap attemps failed.")
            
    def _reconnect(self):
        node_id = random.randint(0, len(self.nodes) - 1)
        num_nodes = len(self.nodes)
        attempts = 0
        self.close()
        while attempts < num_nodes:
            node_id = (node_id + 1) % num_nodes
            new_node = self.nodes[node_id]
            conn = None
            try:
                conn = SocketConnector(self.conflict_resolver)
                conn.open_connection(new_node.host, new_node.socket_port)
                self.connection = conn
                break
            except ConnectionException as e:
                logging.warn("Error connection to node %s:%d." %(host, port))
                attempts += 1
        if not self.connection:
            raise VoldemortException("Connections to all nodes failed.")
