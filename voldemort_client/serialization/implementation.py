class Serializer:
    """
    This is the dummy serializer implementation class which does nothing.
    """

    def __init__(self):
        """
        This is the initialisation method of the class.
        """
        pass

    def read(self, content):
        """
        """
        return ""

    def write(self, content):
        """
        """
        return ""

    @classmethod
    def configure_from_xml(cls, node):
        """
        This method initializes the serializer from a xml node.
        """
        return cls()

class StringSerializer(Serializer):
    """
    This is the string serializer implementation class.
    """

    def __init__(self):
        """
        This is the constructor method of the class.
        """
        pass

    def read(self, content):
        """
        """
        return content

    def write(self, content):
        """
        """
        return content

    @classmethod
    def configure_from_xml(cls, node):
        """
        This builds the string serializer from xml.
        """
        return cls()
