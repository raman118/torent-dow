"""
Bencode encoder and decoder implementation for BitTorrent.
Handles integers, byte strings, lists, and dictionaries.
"""

from typing import Any, Union


def decode(data: bytes) -> Any:
    """
    Decodes bencoded data.
    """
    def decode_next(index: int) -> tuple[Any, int]:
        char = data[index : index + 1]
        if char == b"i":
            # Integer: i<integer>e
            end = data.find(b"e", index)
            if end == -1:
                raise ValueError("Invalid bencode integer: missing 'e'")
            return int(data[index + 1 : end]), end + 1
        elif char == b"l":
            # List: l<elements>e
            index += 1
            elements = []
            while data[index : index + 1] != b"e":
                element, index = decode_next(index)
                elements.append(element)
            return elements, index + 1
        elif char == b"d":
            # Dictionary: d<keys/values>e
            index += 1
            dictionary = {}
            while data[index : index + 1] != b"e":
                key, index = decode_next(index)
                if not isinstance(key, bytes):
                    raise TypeError("Bencode dictionary keys must be byte strings")
                value, index = decode_next(index)
                dictionary[key] = value
            return dictionary, index + 1
        elif char.isdigit():
            # Byte string: <length>:<data>
            colon = data.find(b":", index)
            if colon == -1:
                raise ValueError("Invalid bencode string: missing ':'")
            length = int(data[index:colon])
            start = colon + 1
            end = start + length
            return data[start:end], end
        else:
            raise ValueError(f"Invalid bencode prefix: {char!r}")

    result, _ = decode_next(0)
    return result


def encode(obj: Any) -> bytes:
    """
    Encodes a Python object into bencoded bytes.
    """
    if isinstance(obj, int):
        return b"i" + str(obj).encode() + b"e"
    elif isinstance(obj, bytes):
        return str(len(obj)).encode() + b":" + obj
    elif isinstance(obj, str):
        # Convenience for strings, though protocol uses bytes
        encoded = obj.encode()
        return str(len(encoded)).encode() + b":" + encoded
    elif isinstance(obj, list):
        return b"l" + b"".join(encode(x) for x in obj) + b"e"
    elif isinstance(obj, dict):
        # Keys must be strings (bytes in bencode) and sorted
        items = sorted(obj.items())
        return b"d" + b"".join(encode(k) + encode(v) for k, v in items) + b"e"
    else:
        raise TypeError(f"Object of type {type(obj)} is not bencode serializable")


if __name__ == "__main__":
    # Test cases
    test_data = {
        b"string": b"spam",
        b"number": 42,
        b"list": [b"a", b"b", 3],
        b"dict": {b"foo": b"bar", b"baz": 52},
    }

    encoded = encode(test_data)
    print(f"Encoded: {encoded}")
    decoded = decode(encoded)
    print(f"Decoded: {decoded}")

    assert decoded == test_data
    print("Bencode implementation verified!")
