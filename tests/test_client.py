from voldemort_client.client import VoldemortClient

class TestVoldemortClient:
    """
    This is the test class for the VoldemortClient class.
    """

    def test_get(self):
        """
        Test the basic get method workflow.
        """
        client = VoldemortClient("test", [("localhost", 6666)])
        assert None == client.get("k")
        client.set("k", "v")
        assert  "v" == client.get("k") 
