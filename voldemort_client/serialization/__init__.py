from voldemort_client.serialization.json_serializer import JsonTypeSerializer
from voldemort_client.serialization.implementation import Serializer, StringSerializer

SERIALIZER_CLASSES = {
    "string": StringSerializer,
    "json": JsonTypeSerializer,
}

def build_serializer(serializer_type, node):
    """
    This method builds and initialized the serializer.
    """
    serializer = Serializer.configure_from_xml(node)

    if serializer_type in SERIALIZER_CLASSES:
        class_def = SERIALIZER_CLASSES[serializer_type]
        serializer = class_def.configure_from_xml(node)

    return serializer
