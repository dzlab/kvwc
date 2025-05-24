import pickle
import json
import msgpack
import logging

logger = logging.getLogger(__name__)

class Serializer:
    """
    Base class for value serialization.
    """
    def serialize(self, value):
        """
        Serializes a Python object into bytes.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement serialize method")

    def deserialize(self, value_bytes):
        """
        Deserializes bytes back into a Python object.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement deserialize method")

class StrSerializer(Serializer):
    """
    Serializer for simple string values, encoded as UTF-8 bytes.
    """
    def serialize(self, value):
        """
        Serializes a string value into UTF-8 bytes.
        """
        if value is None:
            return None
        try:
            return str(value).encode('utf-8')
        except Exception as e:
            logger.error(f"Error serializing value '{value}' to string/utf-8: {e}")
            raise

    def deserialize(self, value_bytes):
        """
        Deserializes UTF-8 bytes back into a string.
        """
        if value_bytes is None:
            return None
        try:
            return value_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Error deserializing bytes '{value_bytes.hex()}' to string/utf-8: {e}")
            raise

class PickleSerializer(Serializer):
    """
    Serializer using Python's pickle module. Can handle most Python objects.
    Note: Deserializing untrusted pickle data is insecure.
    """
    def serialize(self, value):
        """
        Serializes a Python object using pickle.
        """
        if value is None:
            return None
        try:
            return pickle.dumps(value)
        except Exception as e:
            logger.error(f"Error serializing value '{value}' using pickle: {e}")
            raise

    def deserialize(self, value_bytes):
        """
        Deserializes bytes using pickle.
        """
        if value_bytes is None:
            return None
        try:
            return pickle.loads(value_bytes)
        except Exception as e:
            # Log a more general error for pickle as data might be from an old format or untrusted
            logger.error(f"Error deserializing bytes using pickle: {e}")
            raise

class JsonSerializer(Serializer):
    """
    Serializer using JSON. Only supports JSON-serializable types.
    """
    def serialize(self, value):
        """
        Serializes a Python object using JSON (to UTF-8 bytes).
        """
        if value is None:
            return None
        try:
            # Use json.dumps and then encode the resulting string
            return json.dumps(value).encode('utf-8')
        except Exception as e:
            logger.error(f"Error serializing value '{value}' using json: {e}")
            raise

    def deserialize(self, value_bytes):
        """
        Deserializes UTF-8 bytes using JSON.
        """
        if value_bytes is None:
            return None
        try:
            # Decode bytes first, then parse JSON string
            json_string = value_bytes.decode('utf-8')
            return json.loads(json_string)
        except Exception as e:
            logger.error(f"Error deserializing bytes '{value_bytes.hex()}' using json: {e}")
            raise

class MsgPackSerializer(Serializer):
    """
    Serializer using MessagePack. Efficient binary serialization.
    """
    def serialize(self, value):
        """
        Serializes a Python object using msgpack.
        """
        if value is None:
            return None
        try:
            # use_bin_type=True is often useful for bytes objects
            return msgpack.packb(value, use_bin_type=True)
        except Exception as e:
            logger.error(f"Error serializing value '{value}' using msgpack: {e}")
            raise

    def deserialize(self, value_bytes):
        """
        Deserializes bytes using msgpack.
        """
        if value_bytes is None:
            return None
        try:
            # raw=False decodes binary strings (bytes) to Python strings (str)
            return msgpack.unpackb(value_bytes, raw=False)
        except Exception as e:
            logger.error(f"Error deserializing bytes '{value_bytes.hex()}' using msgpack: {e}")
            raise
