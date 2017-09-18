import email
import re
import requests
import simplejson as json
from voldemort_client import helper
from voldemort_client.exception import VoldemortException

class VoldemortClient:
    """
    This class manages the connection to a voldemort cluster and provides
    the accessing methods.
    """

    def __init__(self, host, port, store_name, protocol="http",
            request_timeout=3000, origin_time=3000):
        """
        This is the constructor method of the class.
        """
        self._host = host
        self._port = port
        self._store_name = store_name
        self._protocol = protocol
        self._request_timeout = request_timeout
        self._origin_time = origin_time

    def get(self, key):
        """
        This method returns the value, versions pairs from the server.

        :param key: the key to fetch
        :type key: str
        """
        headers = helper.build_get_headers(self._request_timeout, self._origin_time)
        response = requests.get(helper.build_url(self._host, self._port, self._store_name, key, self._protocol), headers=headers)
        response_code = response.status_code
        if response_code == 200:
            print(response.content)
            messages = self._extract_messages(response.content)
            sub_messages = [message.get_payload()[0] for message in messages]
            result = []
            for sub_message in sub_messages:
                header = sub_message.get("X-VOLD-Vector-Clock")
                versiondict = json.loads(header)
                result.append((versiondict["versions"], sub_message.get_payload()))
            return result
        elif response_code == 404:
            return []
        else:
            response.raise_for_status()

     def gets(self, keys):
         """
         This method returns the values from the key list.

         :param keys: the list of keys
         :type keys: list
         """
         return self.get(', '.join(keys))

    def set(self, key, value, node_id = None):
        """
        This method sets the value on the server.

        :param key: the key under which the value should be store
        :type key: str
        :param value: the value to store
        :type value: str
        :param node_id: the id of one node
        :type node_id: int
        """
        fetch_value = self.get(key)
        clock = None
        if len(fetch_value) > 0:
            versions = fetch_value[0][0]
            for versiondict in versions:
                versiondict["version"] = versiondict["version"] + 1
        clock = helper.build_vector_clock(node_id, versions)
        headers = helper.build_set_headers(self._request_timeout, self._origin_time, clock)
        response = requests.post(helper.build_url(self._host, self._port,
            self._store_name, key, self._protocol), headers=headers, data=value)
        response.raise_for_status()

    def delete(self, key):
        """
        This method deletes an existing value.

        :param key: the key to delete
        :type key: str
        """
        fetch_value = self.get(key)
        clock = None
        if len(fetch_value) > 0:
            clock = helper.build_vector_clock(None, fetch_value[0][0])
            headers = helper.build_delete_headers(self._request_timeout, self._origin_time, clock)
            response = requests.delete(helper.build_url(self._host, self._port, self._store_name, key, self._protocol), headers=headers)
            response.raise_for_status()

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
