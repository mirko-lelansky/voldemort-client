import simplejson as json
import time

def build_vector_clock(node_id = None, versions = None):
    """
    """
    if node_id is not None or versions is not None:
        clock = {
            "timestamp": int(time.time()) * 1000
        }
        if node_id is not None:
            clock["versions"] = [{
                "nodeId": node_id,
                "version": 1
            }]
        else:
            clock["versions"] = versions
        return clock
    else:
        raise ValueError("You need the node_id or the versions.")

def build_get_headers(request_timeout, origin_time):
    """
    """
    return {
            "X-VOLD-Request-Timeout-ms": str(request_timeout),
            "X-VOLD-Request-Origin-Time-ms": str(origin_time)
    }

def build_delete_headers(request_timeout, origin_time, vector_clock):
    """
    """
    delete_headers = build_get_headers(request_timeout, origin_time)
    delete_headers["X-VOLD-Vector-Clock"] = json.dumps(vector_clock)
    return delete_headers

def build_set_headers(request_timeout, origin_time, vector_clock, content_type="text/plain"):
    """
    """
    set_headers = build_delete_headers(request_timeout, origin_time, vector_clock)
    set_headers["Content-Type"] = content_type
    return set_headers

def build_url(host, port, store_name, key, protocol="http"):
    """
    """
    return "%s://%s:%d/%s/%s" % (protocol, host, port, store_name, key)
