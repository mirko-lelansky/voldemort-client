import logging
import socket
import struct
import time
from voldemort_client.exception import ConnectionException
from voldemort_client.protocol import voldemort_client_v2_pb2 as protocol

class SocketConnector:
    """
    This class handles the execution of requests and the protobuf marshalling and unmarshalling.
    """

    def __init__(self, conflict_resolver = None):
        """
        This is the initialization method of the class.
        """
        self.connection = None
        self.open = False
        self.request_count = 0
        self.conflict_resolver = conflict_resolver

    def open_connection(self, host, port, protocol="pb0"):
        """
        This method opens a socket connection to the server.
        """
        if not self.connection:
            conn = None
            try:
                logging.debug("Attempt to connect to %s:%d ." % (host, port))
                conn = socket.socket()
                conn.connect((host, port))
                logging.debug("Connection succeeded. Attempt to change the protocol.")
                conn.send(protocol.encode())
                resp = conn.recv(2).decode()
                if resp != "ok":
                    conn.close()
                    raise ConnectionException("The server didn't understand the protocol.")
                logging.debug("Changeing protocol succeeded.")
                self.connection = conn
                self.open = True
            except soccket.error as e:
                if conn:
                    conn.close()
                raise ConnectionException("The connection couldn't be open.")

    def execute_get_request(self, key, store_name, should_route = False):
        """
        This method executes a protobuf get request."

        :param key: the lockup key
        :type key: bytes
        :param store_name: the name of the store
        :type store_name: str
        :param should_route:
        :type should_route: bool
        """
        if self.open:
            request = protocol.VoldemortRequest()
            request.get.key = key
            request.should_route = should_route
            request.store = store_name
            request.type = protocol.GET

            self._send_request(request.SerializeToString())

            response_content = self._received_response()
            response = protocol.GetResponse()
            response.ParseFromString(response_content)

            self._check_error(response)
            return self._extract_versions(response.versioned)
        else:
            raise ConnectionException("The connection is closed you have to call open_connection before you can send request.")

    def execute_put_request(self, key, value, version, node_id, store_name, should_route = True):
        """
        This method executes a protobuf put request.
        """
        if self.open:
            request = protocol.VoldemortRequest()
            request.put.key = key
            request.put.versioned.value = value
            request.put.versioned.version.MergeFrom(version)
            request.should_route = should_route
            request.store = store_name
            request.type = protocol.PUT

            self._send_request(request.SerializeToString())

            response_content = self._received_response()
            response = protocol.PutResponse()
            response.ParseFromString(response_content)

            self._check_error(response)

            return self._increment(version, node_id)
        else:
            raise ConnectionException("The connection is closed you have to call open_connection before you can send requests.")

    def close_connection(self):
        """
        This method closed the socket connection.
        """
        if self.connection:
            try:
                self.connection.close()
                self.open = False
            except socket.error as e:
                raise ConnectionException("The connection couldn't be close.")

    def _send_request(self, request):
        """
        This methods send the request to the voldemort cluster.

        :param request: the request to send
        :type request: protobuf message
        """
        try:
            self.connection.send(struct.pack(">i", len(request)) + request)
            self.request_count += 1
        except socket.error as e:
            raise ConnectionException("The request couldn't be send.")

    def _received_response(self):
        """
        This method received the reponses from the server.
        """
        size_bytes = self.connection.recv(4)
        if not size_bytes:
            self.close_connection()
            raise VoldemortException("The server send any bytes.")
        size = struct.unpack(">i", size_bytes)[0]

        bytes_read = 0
        data = []

        while size and bytes_read < size:
            chunk = self.connection.recv(size - bytes_read)
            bytes_read += len(chunk)
            data.append(chunk)

        return b"".join(data)

    def _check_error(self, response):
        """
        This method checks if any error occured.

        :param response: the response to check
        :type response: protobuf message
        """
        if response.error and response.error.error_code != 0:
            raise ConnectionException(response.error.error_code)

    def _extract_versions(self, response_versions):
        """
        This method extracts the versions from the get requests.

        :param response_versions:
        :type response_versions:
        """
        versions = [(versioned.value, versioned.version) for versioned in response_versions]
        return self._resolve_conflicts(versions)

    def _resolve_conflicts(self, versions):
        """
        This method resolves the version conflicts.

        :param versions:
        :type versions:
        """
        if self.conflict_resolver and versions:
            return self.conflict_resolver(versions)
        else:
            return versions

    def _increment(self, clock, node_id):
        """
        This method increments the version.

        :param clock:
        :type clock:
        """
        new_clock = protocol.VectorClock()
        new_clock.MergeFrom(clock)

        # See if we already have a version for this guy, if so increment it
        for entry in new_clock.entries:
            if entry.node_id == node_id:
                entry.version += 1
                return new_clock

        # Otherwise add a version
        entry = new_clock.entries.add()
        entry.node_id = node_id
        entry.version = 1
        new_clock.timestamp = int(time.time() * 1000)

        return new_clock
