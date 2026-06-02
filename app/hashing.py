import hashlib
import os
def hash_password(text: str) ->str :
    hash_object = hashlib.sha256(text.encode('utf-8'))
    return hash_object.hexdigest()

#scrypt hashing
def scrypt_hashing(text:str) ->str :
    if not isinstance(text,str) or not text :
        raise ValueError("Password must be a non-empty string.")
    
    # Generate 16 random bytes as salt
    salt = os.urandom(16)
    try:
        hash_val = hashlib.scrypt(password=text.encode("utf-8"), salt=salt,n=2**14,r=8,p=1,dklen=64)
        print("pwd =",hash_val)
        return salt.hex() + ":" + hash_val.hex()
    except  Exception as e:
        raise RuntimeError(f"scrypt hashing failed: {e}")
    

def verify_scrypt_hashing(password_text:str,hashed_password:str) ->bool :
    if ':' not in hashed_password:
        raise ValueError("Stored hash format invalid. Expected 'salt:key'.")
    salt_hex,hashed_val_hex=hashed_password.split(':',1)
    try:
        salt=bytes.fromhex(salt_hex)
        hashed_val=bytes.fromhex(hashed_val_hex)
    except ValueError:
        raise ValueError("Stored hash contains invalid hex encoding.")

    try:
        new_hash_val = hashlib.scrypt(password=password_text.encode("utf-8"), salt=salt,n=2**14,r=8,p=1,dklen=64)
        return new_hash_val == hashed_val
    except  Exception as e:
        raise RuntimeError(f"scrypt hashing failed: {e}")
    
def get_password(pw):
    if ':' not in pw:
        raise ValueError("Stored hash format invalid. Expected 'salt:key'.")
    
    return pw.split(':',1)[1]


