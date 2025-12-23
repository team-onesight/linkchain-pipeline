import uuid
from hashlib import md5


def get_uuid_hash(value: str) -> str:
    """
    Generate a UUID hash from the given str
    :param value: given str
    :type value: str
    :return: UUID hash
    :rtype:
    """
    md5_hex = md5(value.encode("utf-8")).hexdigest()  # noqa: S324
    return str(uuid.UUID(md5_hex))
