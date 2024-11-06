import hashlib


class Checksum:

    def __init__(self):
        self._hasher = hashlib.md5()

    def update(self, data: bytes):
        """Updates the checksum with new data"""
        self._hasher.update(data)

    @property
    def hexdigest(self) -> str:
        """Hex digest of data"""
        return self._hasher.hexdigest()
