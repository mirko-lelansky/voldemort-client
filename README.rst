########################
voldemort-client
########################

This is a pure python client to accessing the
`Voldemort Key-Value-Store <http://www.project-voldemort.com/>`_.

=================
Implementation
=================

---------------
Dependencies
---------------

To build and run the project you need python 3.
To build the documentation you need sphinx.
To run the test-suite you need pytest and tox.

---------------
Installation
---------------

To install this project run first :code:`python ./setup.py test` or :code:`tox`
to run the test suite. Then you can
run :code:`python ./setup.py install (--prefix [path])` to install the python
module to you system.

-----------
Usage
-----------

Before you can use the client you have to ensure that the client is
successfully installed. After that you can use the following import statement
:code:`from voldemort_client.client import VoldemortClient` to access the
REST-Client.

Then you can create an instance of the Client with the cluster nodes and the
store name. The cluster nodes must be passed as a list of tuples where the first
tuple entry is the url of the rest endpoint and the second tuple entry is the
node id which you have define in the cluster.xml. Following is an example that
show you the creation of the client
:code:`client = VoldemortClient([("http://localhost:8082/", 1)], "test1")`.

In the server_config folder you find a example cluster configuration which
starts the REST-connector on port 8082. If you want to use this config find the
voldemort-server script in the voldemort bin folder and start the following
command :code:`voldemort-server.{sh|bat} ./server_config/test_cluster`.

----
Api
----

The project documentation is in the folder doc. To build the documentation for
various output formats you can use the Makefile in the doc-folder. If you want
to build only html then you can use the following command
direct :code:`python ./setup.py build_sphinx` .

==============
Contributing
==============

========
License
========

The project is licensed under the Apache 2 License -
see the LICENSE.rst file for details.
