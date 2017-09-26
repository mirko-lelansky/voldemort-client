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
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.message import MIMEMessage
from email.message import Message
import requests_mock
import simplejson as json
from voldemort_client import helper
from voldemort_client.client import VoldemortClient

class TestVoldemortClient:
    """
    This is the test class for the VoldemortClient class.
    """

    def test_get_notexists(self):
        """
        Test the get method with a not existing key.
        """
        with requests_mock.Mocker() as mock:
            mock.get("http://localhost:8082/test1/k", status_code=404)
            client = VoldemortClient([("http://localhost:8082", 0)], "test1")
            result = client.get("k")
            assert None == result

    def test_multiget_notexists(self):
        """
        """
        with requests_mock.Mocker() as mock:
            mock.get("http://localhost:8082/test1/a,b,c", status_code=404)
            client = VoldemortClient([("http://localhost:8082", 0)], "test1")
            result = client.get_many(["a", "b", "c"])
            assert None == result
