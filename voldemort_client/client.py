# Copyright 2017 Mirko Lelansky <mlelansky@mail.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This is the entry module of the project. It contains the base class and some
helper methods.
"""
import email
import logging
import re
import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout
import simplejson as json
from voldemort_client import helper
from voldemort_client.exception import VoldemortError, RestError

class VoldemortClient:
    """This class represents the REST-Client to the voldermort cluster."""

    def __init__(self, servers, store_name, connection_timeout=3000, debug=False,
                 max_length=(None, None)):
        """This is the constructor method of the class.

        Parameters
        ----------
        servers : list
            the list of server tuples (url, node_id)
        store_name : str
            the name of the used store
        connection_timeout : int
            the timeout of the http connection in milli seconds
        debug : bool
            if true print more logging messages
        max_length : tuple
            the tuple of the key and value langth

        Raises
        ------
        ValueError
            If the input parameters not valid.
        """
        if not _is_valid(servers, store_name, debug, connection_timeout):
            raise ValueError("The class isn't correct initialised.")

        self._servers = servers
        self._store_name = store_name
        self._connection_timeout = connection_timeout
        self._debug = debug
        self._max_length = max_length
        self._server_length = len(self._servers)
        self._keys = []

    def add(self, key, value, timeout=None):
        """This method adds on key-value pair on the server but only if the key
        isn't on the server.

        Parameters
        ----------
        key : str
            the key where the value should be stored
        value : str
            the content what should be stored
        timeout : int
            the expire timeout of the key

        Returns
        -------
        bool
            True if success else False
        """
        fetch_value = self.get(key)
        if fetch_value is not None:
            return self.set(key, value, timeout)
        else:
            raise VoldemortError("The key already exists.")

    def clear(self):
        """This method clears all the keys on the cluster."""
        for key in self._keys:
            self.delete(key)
        self._keys.clear()

    def get(self, key):
        """This method returns the value for a specific key.

        Parameters
        ----------
        key : str
            the key to fetch

        Returns
        -------
        str
            the value of the key or None
        """
        headers = helper.build_get_headers(self._connection_timeout)
        content = self._get(key, headers)
        if content:
            message_str = content.decode()
            lines = message_str.split("\r\n")
            message_lines = lines[1:-2]
            msg = '\r\n'.join(message_lines)
            message = email.message_from_string(msg)
            return message.get_payload()

    def get_many(self, keys):
        """This method returns the values from the key list.

        Parameters
        ----------
        keys : list
            the keys to fetch

        Returns
        -------
        dict
            the founded key-value-pairs or None
        """
        headers = helper.build_get_headers(self._connection_timeout)
        content = self._get(','.join(keys), headers)
        if content:
            messages = self._extract_messages(content)
            sub_messages = [(msg.get("Content-Location"), msg.get_payload()[0])
                            for msg in messages]
            result_list = [(sub_message[0].rsplit("/")[2], sub_message[1].get_payload())
                           for sub_message in sub_messages]
            result = {}
            for location, value in result_list:
                for key in keys:
                    if key.startswith(location):
                        result[key] = value
            return result

    def get_version(self, key):
        """This method returns the latest version number of an existing key.

        Parameters
        ----------
        key : str
            the key which should be lockup

        Returns
        -------
        dict
            the version as dict
        """
        headers = helper.build_version_headers(self._connection_timeout)
        content = self._get(key, headers)
        if content:
            return json.loads(content)[0]

    def set(self, key, value, timeout=None):
        """This method sets the value on the server.

        Parameters
        ----------
        key : str
            the key under which the value should be store
        value : str
            the value to store
        timeout : int
            the expire time as timestamp

        Returns
        -------
        bool
            True if success else False
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
                        clock = helper.merge_vector_clock(vector_clock, node_id,
                                                          timeout)
                    headers = helper.build_set_headers(self._connection_timeout,
                                                       clock)
                    response = requests.post(helper.build_url(server,
                                                              self._store_name,
                                                              key),
                                             headers=headers, data=value)
                    response.raise_for_status()
                    return True
                except (ConnectionError, HTTPError, Timeout) as error:
                    if (retries + 1) < self._server_length:
                        self._log("The value couldn't be set on server %s." % server)
                        retries = retries + 1
                        response = None
                    else:
                        self._log("The value couldn't be set.")
                        self._log(error.strerror)
                        return False
        else:
            raise VoldemortError("The key isn't a string.")

    def delete(self, key):
        """This method deletes an existing value.

        Parameters
        ----------
        key : str
            the key to delete

        Returns
        -------
        bool
            True if success else False
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
                                                                    self._store_name,
                                                                    key),
                                                   headers=headers)
                        response.raise_for_status()
                        self._keys.remove(key)
                        return True
                    except (ConnectionError, HTTPError, Timeout) as error:
                        if (retries + 1) < self._server_length:
                            self._log("The value couldn't be deleted on %s." % server)
                            retries = retries + 1
                            response = None
                        else:
                            self._log("The value couldn't be deleted.")
                            self._log(error.strerror)
                            return False
        else:
            raise VoldemortError("The key isn't a string.")

    def _extract_messages(self, response_content):
        """
        """
        parse_content = response_content.decode()
        lines = parse_content.split("\r\n")

        revision_exp = re.compile(r"----=_Part_\d+_\d+.\d+")
        boundary_exp = re.compile(r"(?<=boundary=\")(.*)(?=\")")
        multipart_exp = re.compile(r"Content-Type: multipart/mixed;")

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

    def _get(self, key, headers):
        """
        """
        if isinstance(key, str):
            server = ""
            retries = 0
            response = None
            while retries < self._server_length and response is None:
                try:
                    server = self._servers[retries][0]
                    response = requests.get(helper.build_url(server,
                                                             self._store_name, key),
                                            headers=headers)
                    response.raise_for_status()
                    return response.content
                except (ConnectionError, HTTPError, Timeout) as error:
                    if (retries + 1) < self._server_length:
                        self._log("Couldn't execute the get request on the server: %s." % server)
                        retries = retries + 1
                        response = None
                    else:
                        if isinstance(error, (ConnectionError, Timeout)):
                            raise RestError("No connection couldn't established.")
                        elif isinstance(error, HTTPError) and response.status_code == 404:
                            return []
                        else:
                            raise VoldemortError("An unknown exception occured.")
        else:
            raise VoldemortError("The key isn't a string.")

    def _log(self, msg):
        if self._debug:
            logging.debug(msg)

def _is_valid(servers, store_name, debug, connection_timeout):
    """This method validates the constructor method parameters.

    Parameters
    ----------
    servers : list
        the list of tuples of servers
    store_name : str
        the name of the store to use
    debug : bool
        the flag if the error messages should be printed
    connection_timeout : int
        the timeout for the reuqest

    Returns
    -------
    bool
        True if valid else False
    """
    valid = False
    if _is_valid_servers(servers) and _is_valid_store_name(store_name) and _is_valid_debug(debug) and _is_valid_connection_timeout(connection_timeout):
        valid = True
    return valid

def _is_valid_servers(servers):
    """
    """
    valid = True
    regex_pattern = r"^(https?)://([a-z0-9\-._~%]+|\[[a-z0-9\-._~%°$&'()*+,;=:]+\])(:[0-9]+)?$"
    server_regex = re.compile(regex_pattern)
    if isinstance(servers, list):
        for server in servers:
            if isinstance(server, tuple):
                if isinstance(server[0], str) and isinstance(server[1], int):
                    server_matcher = server_regex.match(server[0])
                    if server_matcher is not None:
                        continue
                    else:
                        valid = False
                        break
                else:
                    valid = False
                    break
            else:
                valid = False
                break
        return valid
    return False

def _is_valid_store_name(store_name):
    """
    """
    return isinstance(store_name, str)

def _is_valid_debug(debug):
    """
    """
    return isinstance(debug, bool)

def _is_valid_connection_timeout(connection_timeout):
    """
    """
    return isinstance(connection_timeout, int)
