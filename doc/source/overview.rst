=========
Overview
=========

This project is a REST-Client for a `Voldemort Cluster <http://www.project-voldemort.com/>`_.

If you want to use this client, you must be ensure the rest port in the cluster
configuration is enabled. This client uses the python requests library to make
the http calls. This fact has the advantage that you have no open connection
like when you use a socket direct like in other clients.

The client provides the following methods add, clear, delete, get, get_many,
get_version, set. The add and set methods put a key-value pair to the cluster.
The difference between the two methods is that add method puts the pair only
if the key didn't exists in the cluster. The get and get_many methods receives
one or more key-value pair. The clear and delete methods remove key-value pairs
from the cluster. The delete method deletes one single element and clear deletes
everything in the cluster. The get_version method is a special retrieve method
which gets the internale version state of a key-value pair. That is special to
voldemort.
