import hashlib
def hash_password(text: str) ->str :
    hash_object = hashlib.sha256(text.encode('utf-8'))
    return hash_object.hexdigest()