from voldemort_client.serialization.common import SerializationException
from voldemort_client.serialization.json_serializer import JsonTypeSerializer
from voldemort_client.serialization.string_serializer import StringSerializer
from voldemort_client.serialization.unimplemented_serializer import UnimplementedSerializer

SERIALIZER_CLASSES = {
    "string": StringSerializer,
    "json": JsonTypeSerializer,
}
