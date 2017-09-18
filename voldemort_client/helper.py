import simplejson as json
from datetime import datetime
from voldemort_client.exception import VoldemortException


def create_vector_clock(node_id, timeout):
    if node_id is not None and timeout is not None:
        return {
            "versions": [{"nodeId": node_id, "version": 1}],
            "timestamp": timeout
        }
    else:
        raise ValueError("You must gave the node id and the timeout.")


def merge_vector_clock(vector_clock, node_id, timeout=None):
    """
    """
    if vector_clock is not None and node_id is not None:
        versions = vector_clock["versions"]
        version_map_list_node = [version_map for version_map in versions if version_map["nodeId"] == node_id]
        if len(version_map_list_node) == 0:
            versions.append({"nodeId": node_id, "version": 1})
        elif len(version_map_list_node) == 1:
            old_map = version_map_list_node[0]
            new_map = old_map
            new_map["version"] = new_map["version"] + 1
            versions.remove(old_map)
            versions.append(new_map)
        else:
            raise VoldemortException("Only one version map per node is allowed.")
        vector_clock["versions"] = versions
        if timeout is not None:
            vector_clock["timestamp"] = timeout
        return vector_clock
    else:
        raise ValueError("You need the vector clock, timeout and the node id.")


def build_get_headers(request_timeout):
    """
    """
    timestamp = datetime.now().timestamp()
    return {
            "X-VOLD-Request-Timeout-ms": str(int(request_timeout * 1000)),
            "X-VOLD-Request-Origin-Time-ms": str(int(timestamp))
    }


def build_delete_headers(request_timeout, vector_clock):
    """
    """
    delete_headers = build_get_headers(request_timeout)
    delete_headers["X-VOLD-Vector-Clock"] = json.dumps(vector_clock)
    return delete_headers


def build_set_headers(request_timeout, vector_clock, content_type="text/plain"):
    """
    """
    set_headers = build_delete_headers(request_timeout, vector_clock)
    set_headers["Content-Type"] = content_type
    return set_headers


def build_version_headers(request_timeout):
    """
    """
    version_headers = build_get_headers(request_timeout)
    version_headers["X-VOLD-Get-Version"] = ""
    return version_headers


def build_url(url, store_name, key):
    """
    """
    return "%s/%s/%s" % (url, store_name, key)
