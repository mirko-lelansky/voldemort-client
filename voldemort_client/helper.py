import simplejson as json
from datetime import datetime


def build_vector_clock(vector_clock, timeout=None, node_id=None, versions=None):
    """
    """
    if vector_clock is not None or (timeout is not None and (node_id is not None or versions is not None)):
        if vector_clock is not None:
            if timeout is not None:
                vector_clock["timeout"] = timeout
            if versions is not None:
                vector_clock["versions"] = versions
            return vector_clock
        else:
            clock = {
                "timestamp": timeout
            }
            if node_id is not None:
                clock["versions"] = [{"nodeId": node_id, "versions": 1}]
            else:
                clock["versions"] = versions
            return clock
    else:
        raise ValueError("You need the timeout value and the node_id or the versions.")


def build_get_headers(request_timeout):
    """
    """
    timestamp = datetime.utcnow().timestamp()
    return {
            "X-VOLD-Request-Timeout-ms": str(request_timeout * 1000),
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


def build_url(url, store_name, key):
    """
    """
    return "%s/%s/%s" % (url, store_name, key)
