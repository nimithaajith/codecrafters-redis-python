
from .data_models import Master,Replica,User,RedisObject
import os
from . import datastore
from datetime import datetime,timedelta,timezone
from collections import deque
###################SERVER START UP################################ 
def aof_replay_file(data_store,aof_path,line) :    
    m_commands=None
    aof_file_name=line.split()[1]    
    aof_file=os.path.join(aof_path,aof_file_name)
    with open(aof_file,'r') as af:
        m_commands=af.read()
    if m_commands :    
        # for m_command in m_commands:
        print(">>>AOF command = ",m_commands)
        input_tokens=m_commands.splitlines()
        i=0        
        commands=deque()
        while i<len(input_tokens):
            tokens=[]
            if input_tokens[i].startswith('*') and len(input_tokens[i]) >1:
                no_of_elements=int(input_tokens[i].lstrip('*')) 
                count=no_of_elements*2
                tokens.append(input_tokens[i])
                for _ in range(count):
                    i=i+1
                    tokens.append(input_tokens[i])
                print(">>>AOF command found = ",tokens)
                commands.append(tokens)                
            i=i+1
        if commands:
            for tokens in commands:           
                data_list=[]            
                for token in tokens:
                    if len(token)>1 and token.startswith('*'):                
                        continue
                    if token.startswith('$') and token.strip() != '$':
                        continue
                    data_list.append(token.strip())
                if data_list:
                    if data_list[0].upper() == 'SET':
                        key=data_list[1]
                        val=data_list[2]
                        data_type = 'string'
                        expiry =None
                        if len(data_list) > 3:
                            if data_list[3] == 'PX':
                                expiry = datetime.now(timezone.utc) + timedelta(milliseconds=int(data_list[4]))
                            elif data_list[3] == 'EX' :
                                expiry = datetime.now(timezone.utc) + timedelta(seconds=int(data_list[4]))                        
                        data_store[key] = RedisObject(data = val,exp=expiry,data_type=data_type)
        else:
            print("NO Commands to replay in AOF")                                        
    return data_store


def setup_server(sys_args,port_number):
    if '--replicaof' in sys_args:        
        RedisAsyncServer=Replica()
    else:        
        RedisAsyncServer=Master()

    if '--port' in sys_args:
        try:            
            port_number=int(sys_args[sys_args.index('--port')+1])
        except:
            port_number=6379
    else:
        port_number=6379

    RedisAsyncServer.clients.append(User())

    if '--replicaof' in sys_args:
        try:
            # RedisAsyncServer.role='slave'                     
            master_details=sys_args[sys_args.index('--replicaof')+1].split(' ')
            master_host = master_details[0].strip()
            master_port = int(master_details[1].strip())
            RedisAsyncServer.master_host=master_host
            RedisAsyncServer.master_port=master_port
            
        except:
            pass
    #--dir /tmp/redis-files --dbfilename dump.rdb
    if '--dir' in sys_args:        
        RDB_DIR=sys_args[sys_args.index('--dir')+1]
        RedisAsyncServer.rdb_dir=RDB_DIR
        RedisAsyncServer.dir=RDB_DIR

    if '--dbfilename' in sys_args:        
        RDB_FILENAME=sys_args[sys_args.index('--dbfilename')+1]
        RedisAsyncServer.rdb_filename=RDB_FILENAME
        
    if '--appendonly' in sys_args:        
        aof_val=sys_args[sys_args.index('--appendonly')+1]
        RedisAsyncServer.appendonly=aof_val
    if '--appenddirname' in sys_args:        
        appenddirname=sys_args[sys_args.index('--appenddirname')+1]
        RedisAsyncServer.appenddirname=appenddirname
    if '--appendfilename' in sys_args:        
        AOFFILENAME=sys_args[sys_args.index('--appendfilename')+1]
        RedisAsyncServer.appendfilename=AOFFILENAME
    if '--appendfsync' in sys_args:        
        aofsyncstat=sys_args[sys_args.index('--appendfsync')+1]
        RedisAsyncServer.appendfsync=aofsyncstat

    if RedisAsyncServer.role=='master':
        if RedisAsyncServer.appendonly == 'yes' :
            aof_dir=os.path.join(RedisAsyncServer.dir,RedisAsyncServer.appenddirname)
            os.makedirs(aof_dir, exist_ok=True)
            new_aof_file=RedisAsyncServer.appendfilename+'.1.incr.aof' 
            manifest_file=RedisAsyncServer.appendfilename+'.manifest'
            aof_filepath=os.path.join(aof_dir,new_aof_file)
            with open(aof_filepath,'a') as f:
                print(f'AOF file check by master......')
            manifest_file_path=os.path.join(aof_dir,manifest_file)
            with open(manifest_file_path,'a') as mf:
                print(f'manifest file check by master')
                data_str=f'file {new_aof_file} seq 1 type i'
                mf.write(data_str)
                
                # data_str=f'file {new_aof_file} seq 1 type i'
                # print("Manifest file created and data written = ",mf.write(data_str))
            
        RedisAsyncServer.master_replid = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        RedisAsyncServer.master_repl_offset = 0
        if RedisAsyncServer.rdb_dir and RedisAsyncServer.rdb_filename:
            RedisAsyncServer=datastore.initialize_data_store(RedisAsyncServer)
    if RedisAsyncServer.role == 'master':
        if RedisAsyncServer.appendonly == 'yes':
            print(">>>>>>>>AOF FILE REPLAY<<<<<<<<")
            aof_path=os.path.join(RedisAsyncServer.dir,RedisAsyncServer.appenddirname)
            print('aof_path =',aof_path)
            if os.path.exists(aof_path) :
                manifest_file=os.path.join(aof_path,RedisAsyncServer.appendfilename+'.manifest')
                print("manifest_file = ",manifest_file)
                if os.path.exists(manifest_file):
                    print("Opening manifest file")
                    with open(manifest_file,'r') as mf:
                        contents=mf.readlines()  
                        print("Contents of manifest file ::",contents)
                        for line in contents:
                            print("line in aof file =",line)
                            if 'type i' in line:
                                print("Calling aof reply......")
                                RedisAsyncServer.data_store=aof_replay_file(RedisAsyncServer.data_store,aof_path,line) 
                                break 
    return RedisAsyncServer,port_number 

#add modifying commands to aof
def add_command(query_string,RedisAsyncServer) :
    aof_dir=os.path.join(RedisAsyncServer.dir,RedisAsyncServer.appenddirname)                    
    manifest_file=RedisAsyncServer.appendfilename+'.manifest'        
    manifest_file_path=os.path.join(aof_dir,manifest_file)
    data_str=None
    with open(manifest_file_path,'r') as mf:
        data_str= mf.readline()
        print("manifest file read =",data_str)
    if data_str is not None:
        aof_file_name=data_str.split()[1]
        aof_file_path=os.path.join(aof_dir,aof_file_name)
        with open(aof_file_path,'a') as af:
            data_cmd=f'{query_string}'
            af.write(data_cmd)    
            print("Added SET to aoffile = ",data_cmd)   
            
               
##########CLIENTS/USERS##########################

def add_client(username,userobjs,client_address):
    for obj in userobjs:
        if username == obj.username:
            userobjs.remove(obj)
            obj.client_address.append(client_address)
            userobjs.append(obj)
    return userobjs


def client_exists(userobjs,client_address):
    addr_list=[]
    for obj in userobjs:
        for client in obj.client_address:
            addr_list.append(client)
    if client_address in addr_list:
        return True
    return False

def allow_commands(userobjs,client_address):
    addr_list=[]
    for obj in userobjs:
        for client in obj.client_address:
            addr_list.append(client)
    if client_address not in addr_list:        
        for user in userobjs:
            if user.username == 'default'  and ('nopass' not in user.flags):
                return False
    return True        


###################STREAMS################################                
def get_last_stream_key(millisecondstime,stream_obj_list):   
    last_sn=-1
    for obj in stream_obj_list:
        mst,sn=([int(part.strip()) for part in str(obj.id).split('-')])
        if millisecondstime == mst:
            if last_sn < sn:
                last_sn=sn
    if millisecondstime == 0 and last_sn == -1:
        last_sn = 0
    print(">>>Last sequence no: ",last_sn)

    return str(millisecondstime)+'-'+str(last_sn+1)


def get_next_stream_key(millisecondstime,stream_obj_list):   
    keydict={}
    last_sn=-1
    for obj in stream_obj_list:
        mst,sn=([int(part.strip()) for part in str(obj.id).split('-')])
        if millisecondstime == mst:
            if last_sn < sn:
                last_sn=sn
    if millisecondstime == 0 and last_sn == -1:
        last_sn = 0
    print(">>>Last sequence no: ",last_sn)

    return str(millisecondstime)+'-'+str(last_sn+1)  


def get_mst_and_sn(stream_key):
    mst=None
    sn=None
    if '-' in stream_key :
        mst= int(stream_key.split('-')[0].strip())
        sn = int(stream_key.split('-')[1].strip())
    else:
        mst= int(stream_key.strip())        
    return mst,sn


def get_xrange_response(redis_obj,start,end):
    
    result=[]
    if start == '-':
        start = redis_obj.data[0].id
        #start set to begining 
    if end == '+':
        end = redis_obj.data[-1].id
        #end set to last
        
    starting_mst,starting_sn =get_mst_and_sn(start)
    print("START>>>",starting_mst,"   ",starting_sn)
    ending_mst,ending_sn=get_mst_and_sn(end)
    print("END>>>",ending_mst,"   ",ending_sn)    
    print('get mst sn completed>>>>>>>')
    for stream_obj in redis_obj.data:
        l=0
        mst,sn =get_mst_and_sn(stream_obj.id) 
        print('CHECKING ID>>>>>>>',stream_obj.id)   
        print(mst,"   ",sn)              
        if starting_mst is not None and starting_sn is not None and ending_mst is not None and ending_sn is not None :
            #'all key parts exists!!!!  
            if mst >= starting_mst and mst <= ending_mst and sn >= starting_sn and sn<= ending_sn:
                l=len(stream_obj.entry)
                result.append((l,stream_obj))
        elif starting_mst is not None and ending_mst is not None and starting_sn is None and ending_sn is None:
            #!!!!no sequence number'
            if all(( mst >= starting_mst, mst <= ending_mst)):
                l=len(stream_obj.entry)
                result.append((l,stream_obj))
        
    n1 = len(result)
    print("result length =",n1)   

    result_str=f'*{n1}\r\n'  
    for length,obj in result:
        print(">>>length,obj =",length,obj)
        result_str=result_str+f'*2\r\n${len(obj.id)}\r\n{obj.id}\r\n*{length*2}\r\n' 
        for k,v in obj.entry.items():
            print(">>>>>>k,v =",k,v)
            result_str=result_str+f'${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n'
    print('XRANGE COMPLETED>>>>>>>')
    return result_str   


async def valid_stream_key(stream_key,last_key):
    #<millisecondsTime>-<sequenceNumber>
    message =''
    stream_key_parts = str(stream_key).split('-')
    stream_key_millisecondsTime = int(stream_key_parts[0].strip())    
    last_key_parts = str(last_key).split('-')
    last_key_millisecondsTime = int(last_key_parts[0].strip())
    last_key_sequenceNumber = int(last_key_parts[1].strip())
    stream_key_sequenceNumber = int(stream_key_parts[1].strip())
    if stream_key_millisecondsTime == 0 and stream_key_sequenceNumber == 0:
        message =f'-ERR The ID specified in XADD must be greater than 0-0\r\n'
    elif last_key_millisecondsTime == stream_key_millisecondsTime :
        if stream_key_sequenceNumber > last_key_sequenceNumber:
            return True,message
        else:
            message=f'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n'
    elif stream_key_millisecondsTime >last_key_millisecondsTime:
        return True,message
    else:
        message=f'-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n'
    return False,message
   

async def get_new_stream_key(stream_key_part1,redis_obj):
    stream_key_millisecondsTime =int(stream_key_part1)
    if not redis_obj.data or not redis_obj.last_key:
        if stream_key_millisecondsTime == 0:
            stream_key = '0-1' 
        else:
            stream_key=stream_key_part1 + '-1'
        
    else:
        stream_key = get_next_stream_key(stream_key_millisecondsTime,redis_obj.data)       
    print(">>>New stream key: ",stream_key)
    return stream_key

def get_xread_response(key,redis_obj,start):
    #XREAD 
    result=[]
    for stream_obj in redis_obj.data:
        l=0
        if stream_obj.id > start:
                l=len(stream_obj.entry)
                result.append((l,stream_obj))  
        
    n1 = len(result)
    print("xread result length =",n1)   

    result_str=f'*2\r\n${len(key)}\r\n{key}\r\n*{n1}\r\n'  
    for length,obj in result:
        #appending each entry object of the stream and number of key-value pairs
        result_str=result_str+f'*2\r\n${len(obj.id)}\r\n{obj.id}\r\n*{length*2}\r\n' 
        for k,v in obj.entry.items():
            # appending key-value pairs of the stream entry
            result_str=result_str+f'${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n'
    print("RESULT = ",result_str)
    return result_str,n1

