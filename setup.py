#!/bin/python3
# Copyright 2017 Mirko Lelansky <mlelansky@mail.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools.command.test import test as TestCommand
from setuptools import setup
from distutils.util import convert_path
import sys

class Tox(TestCommand):
    """
    This is a special command class that runs tox from setup.py
    """
    user_options = [('tox-args=', 'a', "Arguments to pass to tox")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = None

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import tox
        import shlex
        args = self.tox_args
        if args:
            args = shlex.split(self.tox_args)
        errno = tox.cmdline(args=args)
        sys.exit(errno)

def get_version():
    main_ns = {}
    ver_path = convert_path('voldemort_client/version.py')
    with open(ver_path) as ver_file:
        exec(ver_file.read(), main_ns)
    return main_ns['__version__']

def readme():
    with open('README.rst') as f:
        return f.read()

config = dict(
    name='voldemort_client',
    description='Python Voldemort Client',
    long_description=readme(),
    url='http://www.project-voldemort.com',
    author='Mirko Lelansky',
    author_email='mlelansky@mail.de',
    version=get_version(),
    license='Apache',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python 3',
        'Programming Language :: Python 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    keywords='',
    packages=['voldemort_client'],
    platforms=['any'],
    install_requires=["simplejson", "requests"],
    tests_require=['tox'],
    cmdclass={'test': Tox},
    include_package_data=True,
    package_data=dict(
    )
)

if __name__ == "__main__":
    setup(**config)
