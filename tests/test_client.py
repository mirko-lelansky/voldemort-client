from voldemort_client.client import VoldemortClient

class TestVoldemortClient:
    """
    This is the test class for the VoldemortClient class.
    """

    def test_get(self):
        """
        Test the basic get method workflow.
        """
        client = VoldemortClient("localhost", 8082, "test")
        result = client.get("k")
        assert  [] == result
