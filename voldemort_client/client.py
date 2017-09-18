import base64
import email
import requests
import simplejson as json
import time
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
        key_byte = base64.b64encode(key.encode())
        headers = {
                "X-VOLD-Request-Timeout-ms": str(self._request_timeout),
                "X-VOLD-Request-Origin-Time-ms": str(self._origin_time)
        }
        response = requests.get("%s://%s:%d/%s/%s" % (self._protocol,
            self._host, self._port, self._store_name, key_byte), headers=headers)
        if response.status_code == 200:
            content = response.content.decode()
            lines = content.splitlines()
            text = '\r\n'.join(lines[1:-1])
            msg = email.message_from_string(text)
            vector_clock = json.loads(msg.get("X-VOLD-VECTOR-CLOCK"))
            return (vector_clock["versions"], msg.get_payload())
        elif response.status_code == 404:
            return []
        else:
            response.raise_for_status()

     def gets(self, keys):
         """
         This method returns the values from the key list.
         
         :param keys: the list of keys
         :type keys: list
         """
         return self._get(', '.join(keys))

    def set(self, key, value, node_id = None, versions = None):
        """
        This method sets the value on the server.

        :param key: the key under which the value should be store
        :type key: str
        :param value: the value to store
        :type value: str
        :param node_id: the id of one node
        :type node_id: int
        :param versions: the list of versions which entries are dict with the
        keys nodeId and version
        :type version: list
        """
        key_byte = base64.b64encode(key.encode())
        fetch_value = self.get(key)
        clock = None
        if len(fetch_value) > 0:
            versions = fetch_value[0]
            for versiondict in versions:
                versiondict["version"] = versiondict["version"] + 1
                clock = {
                    "versions": versions,
                    "timestamp": time.time() * 1000
                }
        else:
            if versions:
                clock = {
                    "versions": versions,
                    "timestamp": time.time() * 1000
                }
            elif node_id:
                clock = {
                    "versions": [{
                        "nodeId": node_id,
                        "version": 1
                    }],
                    "timestamp": time.time() * 1000
                }
            else:
                raise VoldemortException("The key must exists or you must give the node id or the versions.")
        headers = {
            "X-VOLD-Request-Timeout-ms": str(self._request_timeout),
            "X-VOLD-Request-Origin-Time-ms": str(self._origin_time),
            "X-VOLD-Vector-Clock": json.dumps(clock),
            "Content-Type": "text/plain"
        }
        response = requests.post("%s://%s:%d/%s/%s" % (self._protocol,
            self._host, self._port, self._store_name, key_byte),
            headers=headers, data=value)
        response.raise_for_status(

    def delete(self, key, versions = None):
        """
        This method deletes an existing value.

        :param key: the key to delete
        :type key: str
        """
        key_byte = base64.b64encode(key.encode())
        fetch_value = self.get(key)
        clock = None
        if len(fetch_value) > 0:
            if versions:
                clock = {
                    "versions": versions,
                    "timestamp": time.time() * 1000
                }
            else:
                clock = {
                    "versions": fetch_value[0],
                    "timestamp": time.time() * 1000
                }
            headers = {
                "X-VOLD-Request-Timeout-ms": str(self._request_timeout),
                "X-VOLD-Request-Origin-Time-ms": str(self._origin_time),
                "X-VOLD-Vector-Clock": json.dumps(clock),
                "Content-Type": "text/plain"
            }
            response = requests.delete("%s://%s:%d/%s/%s" % (self._protocol,
                self._host, self._port, self._store_name, key_byte),
                headers=headers)
            response.raise_for_status()
