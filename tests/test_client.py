from email import encoders
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
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
            client = VoldemortClient("localhost", 8082, "test1")
            result = client.get("k")
            assert [] == result

    def test_get_exists(self):
        """
        Test the get method with an existing key.
        """
        with requests_mock.Mocker() as mock:
            expected = MIMEMultipart()
            part1 = MIMEApplication("v", _encoder=encoders.encode_7or8bit)
            part1.add_header("X-VOLD-Vector-Clock", json.dumps(helper.build_vector_clock(0, None)))
            expected.attach(part1)
            mock.get("http://localhost:8082/test1/k", status_code=200, text=expected.as_string())
            client = VoldemortClient("localhost", 8082, "test1")
            result = client.get("k")
            assert "v" == result[0][1]
