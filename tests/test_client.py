from voldemort_client.client import VoldemortClient

class TestVoldemortClient:
    """
    This is the test class for the VoldemortClient class.
    """

    def test_get(self, client):
        """
        Test the basic get method workflow.
        """
        assert None == client.get("k")
        client.set("k", "v")
        assert  "v" == client.get("k")[0]
