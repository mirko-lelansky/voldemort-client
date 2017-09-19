=====
Usage
=====

If you want to use the client class :py:class:`voldemort_client.client.VoldemortClient`,
you have to import them first. The  constructor method takes some mandatory
parameters and some optional parameters. The important mandatory parameter is
the server list. The servers list is a list of tuples. The first value of the
tuple is the url of the REST-API endpoint of one node. The second value is the
node id of the node which REST-API endpoint matches the first value of your
tuple. The second mandatory value is the store_name. The store_name is the id of
the specific backend what you define for your cluster. In your cluster you can
have multiple stores defined but the client can handle only one store at the
same time.

The default connenction timeout of the client is 3000m=3s. If the debug flag is
enabled you get more messages. The other parameters are currently not used, but
will be used in the future to prevent high keys and values.

When you have a client object you can make requests to the voldemort cluster.
