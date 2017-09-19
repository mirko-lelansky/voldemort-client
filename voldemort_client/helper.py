"""
This module contains some helper methods for building parts of http requests.
"""
from datetime import datetime
import simplejson as json
from voldemort_client.exception import VoldemortError


def create_vector_clock(node_id, timeout):
    """This method builds the initial vector clock for a new key.

    Parameters
    ----------
    node_id : int
        the id of one node in the cluster
    timeout : int
        the expire timeout of the key

    Returns
    -------
    dict
        the vector clock as dictonary
    """
    if node_id is not None and timeout is not None:
        return {
            "versions": [{"nodeId": node_id, "version": 1}],
            "timestamp": timeout
        }
    else:
        raise ValueError("You must gave the node id and the timeout.")


def merge_vector_clock(vector_clock, node_id, timeout=None):
    """This method merges an existing vector clock with the new values.

    Parameters
    ----------
    vector_clock : dict
        the vector clock which should be updated
    node_id : int
        the node id to use
    timeout : int
        the expire timeout of the key

    Returns
    -------
    dict
        the update vector clock as dictionary
    """
    if vector_clock is not None and node_id is not None:
        versions = vector_clock["versions"]
        version_map_list_node = [version_map for version_map in versions
                                 if version_map["nodeId"] == node_id]
        if version_map_list_node == []:
            versions.append({"nodeId": node_id, "version": 1})
        elif len(version_map_list_node) == 1:
            old_map = version_map_list_node[0]
            new_map = old_map
            new_map["version"] = new_map["version"] + 1
            versions.remove(old_map)
            versions.append(new_map)
        else:
            raise VoldemortError("Only one version map per node is allowed.")
        vector_clock["versions"] = versions
        if timeout is not None:
            vector_clock["timestamp"] = timeout
        return vector_clock
    else:
        raise ValueError("You need the vector clock, timeout and the node id.")


def build_get_headers(request_timeout):
    """This method builds the request headers for get requests like receving keys.

    Parameters
    ----------
    request_timeout : int
        the time where the request should be done in milli seconds

    Returns
    -------
    dict
        the headers as dictonary
    """
    timestamp = datetime.now().timestamp()
    return {
        "X-VOLD-Request-Timeout-ms": str(int(request_timeout)),
        "X-VOLD-Request-Origin-Time-ms": str(int(timestamp))
    }


def build_delete_headers(request_timeout, vector_clock):
    """This method builds the request headers for the delete requests.

    Parameters
    ----------
    request_timeout : int
        the time where the request should be done in milli seconds
    vector_clock : dict
        the vector clock which represents the version which should be delete

    Returns
    -------
    dict
        the headers as dictionary
    """
    delete_headers = build_get_headers(request_timeout)
    delete_headers["X-VOLD-Vector-Clock"] = json.dumps(vector_clock)
    return delete_headers


def build_set_headers(request_timeout, vector_clock, content_type="text/plain"):
    """This method builds the request headers for the set requests.

    Parameters
    ----------
    request_timeout : int
        the time where the request should be done in milli seconds
    vector_clock : dict
        the vector clock which represents the version which should be create or
        update
    content_type : str
        the content type of the value

    Returns
    -------
    dict
        the headers as dictionary
    """
    set_headers = build_delete_headers(request_timeout, vector_clock)
    set_headers["Content-Type"] = content_type
    return set_headers


def build_version_headers(request_timeout):
    """This method builds the request headers for the version requests.

    Parameters
    ----------
    request_timeout : int
        the time where the request should be done in milli seconds

    Returns
    --------
    dict
        the headers as dictionary
    """
    version_headers = build_get_headers(request_timeout)
    version_headers["X-VOLD-Get-Version"] = ""
    return version_headers


def build_url(url, store_name, key):
    """This method combine the different parts of the urls to build the url to
    acces the REST-API.

    Parameters
    ----------
    url : str
        the base url
    store_name : str
        the name of the voldemort store
    key : str
        the url part which represents the key or keys

    Returns
    -------
    str
        the combined url of the REST-API
    """
    return "%s/%s/%s" % (url, store_name, key)
