import email
import logging
import re
import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout
import simplejson as json
from voldemort_client import helper
from voldemort_client.exception import VoldemortException, ConnectionException

class VoldemortClient:
    """
    This class manages the connection to a voldemort cluster and provides
    the accessing methods.
    """

    def __init__(self, servers, store_name, connection_timeout=3, debug=False,
                 server_max_key_length=None, server_max_value_length=None):
        """
        This is the constructor method of the class.

        :param servers: the list of server tuples (url, node_id)
        :type servers: list
        :param store_name: the name of the used store
        :type store_name: str
        :param connection_timeout: the timeout of the http connection in seconds
        :type connection_timeout: int
        :param debug: if true print more logging messages
        :type debug: bool
        :param server_max_key_length: the max length of the key
        :type server_max_key_length: int
        :param server_max_value_length: the max length of the value
        :type server_max_value_length: int
        """
        if not self._is_valid(servers, store_name, debug, connection_timeout):
            raise ValueError("The class isn't correct initialised.")

        self._servers = servers
        self._store_name = store_name
        self._connection_timeout = connection_timeout
        self._debug = debug
        self._server_max_key_length = server_max_key_length
        self._server_max_value_length = server_max_value_length
        self._server_length = len(self._servers)
        self._keys = []

    def add(self, key, value, timeout=None):
        """
        """
        fetch_value = self.get(key)
        if fetch_value is not None:
            return self.set(key, value, timeout)
        else:
            raise VoldemortException("The key already exists.")

    def clear(self):
        for key in self._keys:
            self.delete(key)
        self._keys.clear()

    def get(self, key):
        """
        This method returns the value, versions pairs from the server.

        :param key: the key to fetch
        :type key: str
        """
        headers = helper.build_get_headers(self._connection_timeout)
        content = self._get(key, headers)
        if not content:
            return None
        else:
            message_str = content.decode()
            lines = message_str.split("\r\n")
            message_lines = lines[1:-2]
            msg = '\r\n'.join(message_lines)
            message = email.message_from_string(msg)
            return message.get_payload()

     def get_all(self, keys):
         """
         This method returns the values from the key list.

         :param keys: the list of keys
         :type keys: list
         """
         headers = helper.build_get_headers(self._connection_timeout)
         content = self._get(','.join(keys), headers)
         if not content:
             return None
         else:
             messages = self._extract_messages(content)
             sub_messages = [message.get_payload()[0] for message in messages]
             return [(sub_message.get_payload(), json.loads(sub_message.get("X-VOLD-Vector-Clock"))) for sub_message in sub_messages]

    def get_version(self, key):
        """
        """
        headers = helper.build_version_headers(self._connection_timeout)
        content = self._get(key, headers)
        if not content:
            return None
        else:
            return json.loads(content)[0]

    def set(self, key, value, timeout):
        """
        This method sets the value on the server.

        :param key: the key under which the value should be store
        :type key: str
        :param value: the value to store
        :type value: str
        :param timeout: the expire time as timestamp
        :type timeout: int or None
        """
        if isinstance(key, str):
            server = ""
            retries = 0
            response = None
            clock = None
            while retries < self._server_length and response is None:
                try:
                    server = self._servers[retries][0]
                    node_id = self._servers[retries][1]
                    vector_clock = self.get_version(key)
                    if vector_clock is None:
                        clock = helper.create_vector_clock(node_id, timeout)
                    else:
                        clock = helper.merge_vector_clock(vector_clock, node_id, timeout)
                    headers = helper.build_set_headers(self._connection_timeout,
                                                       clock)
                    response = requests.post(helper.build_url(server,
                                                          self._store_name, key), headers=headers, data=value)
                    response.raise_for_status()
                    return True
                except (ConnectionError, HTTPError, Timeout) as error:
                    if (retries + 1) < self._server_length:
                        self.debug("The value couldn't be set on server %s." % server)
                        retries = retries + 1
                        response = None
                    else:
                        self.debug("The value couldn't be set.")
                        return False
        else:
            raise VoldemortException("The key isn't a string.")

    def delete(self, key):
        """
        This method deletes an existing value.

        :param key: the key to delete
        :type key: str
        """
        if isinstance(key, str):
            server = ""
            retries = 0
            response = None
            vector_clock = self.get_version(key)
            if vector_clock is not None:
                while retries < self._server_length and response is None:
                    try:
                        server = self._servers[retries][0]
                        node_id = self._servers[retries][1]
                        clock = helper.merge_vector_clock(vector_clock, node_id)
                        headers = helper.build_delete_headers(self._connection_timeout,
                                                              clock)
                        response = requests.delete(helper.build_url(server,
                            self._store_name, key), headers=headers)
                        response.raise_for_status()
                        self._keys.remove(key)
                        return True
                    except (ConnectionError, HTTPError, Timeout) as error:
                        if (retries + 1) < self._server_length:
                            self.debug("The value couldn't be deleted on %s." % server)
                            retries = retries + 1
                            response = None
                        else:
                            self.debug("The value couldn't be deleted.")
                            return False
        else:
            raise VoldemortException("The key isn't a string.")

    def _extract_messages(self, response_content):
        """
        """
        parse_content = response_content.decode()
        lines = parse_content.split("\r\n")

        revision_exp = re.compile("----=_Part_\d+_\d+.\d+")
        boundary_exp = re.compile("(?<=boundary=\")(.*)(?=\")")
        multipart_exp = re.compile("Content-Type: multipart/mixed;")

        all_boundaries = boundary_exp.findall(parse_content)

        result = []
        for line in lines:
            revision_matcher = revision_exp.search(line)
            boundary_matcher = boundary_exp.search(line)
            if revision_matcher is None or boundary_matcher is not None:
                result.append(line)
            elif revision_matcher is not None and boundary_matcher is None:
                for boundary in all_boundaries:
                    rev_exp = re.compile(boundary)
                    if rev_exp.search(line) is not None:
                        result.append(line)
        text = '\r\n'.join(result)
        start_index_list = [match.start() for match in multipart_exp.finditer(text)]
        messages_text = []
        for index, value in enumerate(start_index_list):
            if index < len(start_index_list) - 1:
                messages_text.append(text[value:start_index_list[index+1]])
            else:
                messages_text.append(text[value:])
        return [email.message_from_string(message) for message in messages_text]

    def _is_valid(self, servers, store_name, debug, connection_timeout):
        valid = False
        if self._is_valid_servers(servers) and self._is_valid_store_name(store_name) and self._is_valid_debug(debug) and self._is_valid_connection_timeout(connection_timeout):
            valid = True
        return valid

    def _is_valid_servers(self, servers):
        server_regex = re.compile("^(https?)://([a-z0-9\-._~%]+|\[[a-z0-9\-._~%°$&'()*+,;=:]+\])(:[0-9]+)?$")
        if isinstance(servers, list):
            for server in servers:
                if isinstance(server, tuple):
                    if isinstance(server[0], str) and isinstance(server[1], int):
                        server_matcher = server_regex.match(server[0])
                        if server_matcher is not None:
                            continue
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            return True
        else:
            return False

    def _is_valid_store_name(self, store_name):
        return isinstance(store_name, str)

    def _is_valid_debug(self, debug):
        return isinstance(debug, bool)

    def _is_valid_connection_timeout(self, connection_timeout):
        return isinstance(connection_timeout, int)

    def _get(self, key, headers):
        if isinstance(key, str):
            server = ""
            retries = 0
            response = None
            while retries < self._server_length and response is None:
                try:
                    server = self._servers[retries][0]
                    response = requests.get(helper.build_url(server,
                                                             self._store_name, key), headers=headers)
                    response.raise_for_status()
                    return response.content
                except (ConnectionError, HTTPError, Timeout) as error:
                    if (retries + 1) < self._server_length:
                        self.debug("Couldn't execute the get request on the server: %s." % server)
                        retries = retries + 1
                        response = None
                    else:
                        if isinstance(error, ConnectionError) or isinstance(error, Timeout):
                            raise ConnectionException("No connection couldn't established.")
                        elif isinstance(error, HTTPError) and response.status_code == 404:
                            return []
                        else:
                            raise VoldemortException("An unknown exception occured.")
        else:
            raise VoldemortException("The key isn't a string.")

    def debug(self, msg):
        if self._debug:
            logging.debug(msg)
