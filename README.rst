########################
voldemort-client
########################

This is a pure python client to accessing the voldemort key/value store.

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

To install this project run first:

    python ./setup.py test

or:

    tox

to ensure that all tests are run successfully. Then run:

    python ./setup.py install (--prefix [path])

to install the project. You can set the installation path by using the prefix
option.

-----------
Usage
-----------

{usage instruction}

----
Api
----

The project documentation is in the folder doc. To build the documentation for
various output formats you can use the Makefile in the doc-folder. If you want
to build only html then you can use the following command directl:

    python ./setup.py build_sphinx

.

==============
Contributing
==============

========
License
========

The project is licensed under the Apache 2 License -
see the LICENSE.rst file for details.
