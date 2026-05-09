import os
import shutil
from datetime import timezone,datetime
from .data_models import RedisObject
#initialize datastore from RDB snapshot instance
def initialize_data_store(RedisAsyncServer):
    try:        
        rdb_dir=RedisAsyncServer.rdb_dir
        rdb_filename=RedisAsyncServer.rdb_filename
        os.makedirs(rdb_dir, exist_ok=True)
        filepath=os.path.join(rdb_dir,rdb_filename)
        basedir="C:\\Users\\Ardra\\codecrafters-redis-python"
        os.makedirs(basedir, exist_ok=True)  
        tempfilepath=os.path.join(basedir,rdb_filename) 
            
        with open(tempfilepath,'wb') as dst,open(filepath,'rb') as src:
            shutil.copyfileobj(src, dst) 
        # b'REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfe\x00\xfb\x01\x00\x00\x05mango\x06orange\xff\xeb)\xe1\xcfp\x08\x1f\x9a'
        with open(tempfilepath,'rb') as rdbfile:
            print(rdbfile.read(1024))
        # tempfilepath='./app/temp.rdb'
        # with open(tempfilepath,'wb') as rdbfile:
        #     rdbfile.write(b'REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfe\x00\xfb\x04\x00\x00\x05grape\x06orange\x00\traspberry\tpineapple\x00\x06orange\x05mango\x00\tblueberry\x06banana\xffue\xb86\xd9\x03\xaa8')
        with open(tempfilepath,'rb') as rdbfile: 
            chunk = rdbfile.read(5)  
            if chunk == b'REDIS' :
                print("Reading rdb file, REDIS found !!")  
            key_count=0
            expiring_key_count=0
            while i := rdbfile.read(1):
                if i==b'\xfe':
                    print("New db found !!")  
                    while i := rdbfile.read(1):
                        if i==b'\xfb':
                            key_count=rdbfile.read(1)[0]
                            print("Key count= ",key_count)  
                            expiring_key_count=rdbfile.read(1)[0]
                            break
                    break 
            if key_count: 
                k_count= key_count-expiring_key_count  
                while data := rdbfile.read(1):
                    # create=False                    
                    if data == b'\xff':
                        break
                    if expiring_key_count:
                        if data == b'\xfd':
                            print(">>>>Expire timestamp in seconds key-value found")                            
                            #Expire timestamp in seconds (4-byte unsigned integer)
                            expires_on_sec = int.from_bytes(rdbfile.read(4), byteorder='little', signed=False)
                            expiry = datetime.fromtimestamp(expires_on_sec,tz=timezone.utc)
                            # expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_on_sec)                
                            value_type= rdbfile.read(1)[0]
                            type = RedisAsyncServer.get_type(value_type)
                            key_len=rdbfile.read(1)[0]
                            key=rdbfile.read(key_len).decode()
                            val_len=rdbfile.read(1)[0]
                            val=rdbfile.read(val_len).decode()
                            if datetime.now(timezone.utc) < expiry :
                                RedisAsyncServer.data_store[key] = RedisObject(data = val,exp=expiry,data_type=type) 
                                print('=======saved========') 
                                print(f'type({type}) --->{key} : {val} expires on {expiry}') 
                            expiring_key_count -=1              
                            
                            
                        elif data == b'\xfc' :
                            print(">>>Expire timestamp in milliseconds key-value found")
                            #Expire timestamp in milliseconds (8-byte unsigned long)
                            expires_on_msec = int.from_bytes(rdbfile.read(8), byteorder='little', signed=False)
                            expiry = datetime.fromtimestamp(expires_on_msec / 1000,tz=timezone.utc)
                            # expiry = datetime.now(timezone.utc) + timedelta(milliseconds=expires_on_sec)
                            value_type= rdbfile.read(1)[0]
                            type = RedisAsyncServer.get_type(value_type)
                            key_len=rdbfile.read(1)[0]
                            key=rdbfile.read(key_len).decode()
                            val_len=rdbfile.read(1)[0]
                            val=rdbfile.read(val_len).decode()
                            if datetime.now(timezone.utc) < expiry :
                                RedisAsyncServer.data_store[key] = RedisObject(data = val,exp=expiry,data_type=type) 
                                print('=======saved========') 
                                print(f'type({type}) --->{key} : {val} expires on {expiry}') 
                            expiring_key_count -=1                        
                    else :
                        print(">>>>non expiriny key-value found")                        
                        # data without expiry
                        expiry=None
                        bytetype=data
                        for i in range(k_count):                            
                            value_type= bytetype[0]
                            type = RedisAsyncServer.get_type(value_type)
                            # print("type = ",type)
                            key_len=rdbfile.read(1)[0]
                            # print("key_len=",key_len)
                            key=rdbfile.read(key_len).decode()
                            # print("key= ",key)
                            val_len=rdbfile.read(1)[0]
                            # print("val_len= ",val_len)
                            val=rdbfile.read(val_len).decode()
                            # print("val= ",val)                     
                            RedisAsyncServer.data_store[key] = RedisObject(data = val,exp=expiry,data_type=type) 
                            print('=======saved========') 
                            print(f'type({type}) --->{key} : {val}') 
                            bytetype=rdbfile.read(1)
                        data = bytetype 
                    if data == b'\xff':
                        break                                        
    except Exception as e:
        print("Exception during rdb file save :: ",e) 

    return RedisAsyncServer  
