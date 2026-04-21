import asyncio
from datetime import timezone,timedelta,datetime
import time
import threading
from collections import deque,defaultdict
import os
import json
import shutil
class RedisServer():
    def __init__(self,role='master',port=6379):
        self.port= port
        self.host='localhost'
        self.role = role        
        self.master_host=None
        self.master_port=None
        self.data_store={}
        self.server=Master()

class Replica():
    def __init__(self):
        self.replica_command_offset=0
        print('replica offset initialized to 0')
        

class Master():
    def __init__(self):
        self.rdb_filename=None
        self.rdb_dir=None
        self.master_replid = ''
        self.master_repl_offset=None
        
        # dict of Lists,key is slave-writer,value is list(replica's offset,sync)
        # sync is true means replica's offset and  replica server's replica_command_offset are same
        self.ReplicaList={}
    def get_type(self,value):
        if value == 0:
            return 'string'
        if value == 1:
            return 'list'
        if value == 2:
            return 'set'
        return 'string'
def initialize_data_store():
    try:
        rdb_dir=RedisAsyncServer.server.rdb_dir
        rdb_filename=RedisAsyncServer.server.rdb_filename
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
                            type = RedisAsyncServer.server.get_type(value_type)
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
                            type = RedisAsyncServer.server.get_type(value_type)
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
                            type = RedisAsyncServer.server.get_type(value_type)
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

    

RedisAsyncServer=RedisServer()
channel_subscriptions={}

CommandDeque=deque()
class RedisObject():
    def __init__(self,data=None,data_type=None,exp=None,counter=0):
        self.data = data
        self.exp = exp
        self.counter = counter
        self.data_type = data_type
        self.last_key=None
        self.blocked_clients=deque() #deque object

    def add_data(self,data):
        self.data = data

    def add_data_type(self,data_type):
        self.data_type = data_type

    def incr_counter(self):
        self.counter += 1

    def decr_counter(self):
        self.counter -= 1

    def add_exp(self,exp):
        self.exp = exp
    

class StreamEntry():
    def __init__(self,id):
        self.id = id
        self.entry={} 
    def add_entry(self,data_list) :
        print("******Add entry called*******")
        for item in data_list:
            self.entry[item[0]] = item[1] 
            print("stream entry added key-val as :",item[0],item[1])
            print("stream entry dict")  
            print(self.entry)
        print("******Add entry finished*******")

# data_store={}
xread_stream_block_que=defaultdict(list)

async def blocked_client_handler():
    print("############blocked_client_handler#############")
    while True:
        
        if RedisAsyncServer.data_store:
            for key in RedisAsyncServer.data_store:
                if RedisAsyncServer.data_store[key].blocked_clients:
                    blocked_clients=RedisAsyncServer.data_store[key].blocked_clients
                    try:
                        for client_tuple in blocked_clients:
                            try:
                                _,expires_on,_=client_tuple
                                if datetime.now(timezone.utc) >= expires_on:
                                    RedisAsyncServer.data_store[key].blocked_clients.remove(client_tuple)
                                    print("******REMOVED BLPOP CLIENT*******")
                                    # response='*-1\r\n'
                                    # client_writer.write(response.encode())
                                    # await client_writer.drain()
                            except:
                                pass 
                    except:
                        pass                   
                        

        await asyncio.sleep(0.5)   # check twice per second
                    
async def get_blpop_response(client_tuple) :
    key =client_tuple[2]
    redis_obj=RedisAsyncServer.data_store[key]
    SERVERD=False
    while not SERVERD:
        if client_tuple in redis_obj.blocked_clients:
            if client_tuple == redis_obj.blocked_clients[0] and redis_obj.data:
                value = redis_obj.data.pop(0)
                redis_obj.blocked_clients.popleft()
                response=f'*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n'   
                SERVERD=True                                      
                return response
            

            else:
                await asyncio.sleep(0.01)
        else:
            response = '*-1\r\n'
            return response  




def get_milliseconds_time():
    return int(time.time() * 1000)

def get_mst_and_sn(stream_key):
    mst=None
    sn=None
    if '-' in stream_key :
        mst= int(stream_key.split('-')[0].strip())
        sn = int(stream_key.split('-')[1].strip())
    else:
        mst= int(stream_key.strip())        
    return mst,sn

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
        

async def get_data_type(val):
    if isinstance(val,str):
        return 'string'
    else:
        None


async def xread_stream_block_handler(key,stream_key,expires_on,client_addr):
    while True:        
        #return response,expired
        if datetime.now(timezone.utc) >= expires_on :
            xread_stream_block_que[key].remove(client_addr)
            return  f'*-1\r\n',True
        else:
            client_details_list=xread_stream_block_que[key]
            if client_addr in client_details_list:
                redis_obj=RedisAsyncServer.data_store[key]
                if redis_obj.data:
                    for stream_obj in redis_obj.data:       
                        if stream_obj.id > stream_key:                        
                            response,n1= get_xread_response(key,redis_obj,stream_key)                    
                            xread_stream_block_que[key].remove(client_addr)                   
                            return response,False                    
        await asyncio.sleep(0.01)

async def propagate_command():
    while len(CommandDeque)>0:
        command_str=CommandDeque.popleft()
        # print(">>>PROPAGATING>>>>writer status ",command_str)
        cmd_encoded=command_str.encode()
        for s_writer in RedisAsyncServer.server.ReplicaList.keys():             
            s_writer.write(cmd_encoded)
            await s_writer.drain()            
            curr_offset=RedisAsyncServer.server.ReplicaList[s_writer][0]
            # print("server side offset of replica =",curr_offset)
            # server side update of offset
            RedisAsyncServer.server.ReplicaList[s_writer][0] = curr_offset+len(cmd_encoded)
            # print("server side  new offset of replica =",RedisAsyncServer.server.ReplicaList[s_writer][0])
            
            RedisAsyncServer.server.ReplicaList[s_writer][1]=False
            print("Updated offset and sync to false by master")
            # await asyncio.sleep(0.001)

async def process_synced_replicas(synced_replicas,replica_temp_list,no_of_awaited_replicas):
    for s_writer in RedisAsyncServer.server.ReplicaList.keys():
        if s_writer in replica_temp_list:
            if RedisAsyncServer.server.ReplicaList[s_writer][1]:
                synced_replicas += 1
                replica_temp_list.remove(s_writer)
    return synced_replicas,replica_temp_list

async def propagate_getack_command(replica_temp_list):
    cmd_encoded= b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'
    for s_writer in RedisAsyncServer.server.ReplicaList.keys():
        if s_writer in replica_temp_list:                
            s_writer.write(cmd_encoded)
            await s_writer.drain()            
            curr_offset=RedisAsyncServer.server.ReplicaList[s_writer][0]
            # server side update of offset
            RedisAsyncServer.server.ReplicaList[s_writer][0] = curr_offset+len(cmd_encoded)
            RedisAsyncServer.server.ReplicaList[s_writer][1] = False
        
    

            
async def get_ack_replicas(no_of_awaited_replicas,timeout,waittime):
    replica_temp_list=list(RedisAsyncServer.server.ReplicaList.keys()) 
    synced_replicas=0    
    while True:
        if CommandDeque:
            await asyncio.sleep(0.001)
        else:
            break
    SENDACK=True    
    while datetime.now(timezone.utc) < timeout:
        
        synced_replicas,replica_temp_list=await process_synced_replicas(synced_replicas,replica_temp_list,no_of_awaited_replicas)
           
        if synced_replicas>=no_of_awaited_replicas:
            print("matched synced_replicas ")
            return synced_replicas
        if datetime.now(timezone.utc) >= timeout:
            print("TIME OUT in get_ack_replicas ")
            return synced_replicas
        if SENDACK:
            await propagate_getack_command(replica_temp_list)
            SENDACK=False
        await asyncio.sleep(0.001)
    return synced_replicas    

    
    # while synced_replicas < no_of_awaited_replicas : 
    #     while datetime.now(timezone.utc) < timeout:
    #         synced_replicas,replica_temp_list=await process_synced_replicas(synced_replicas,replica_temp_list,no_of_awaited_replicas)
    #         # print("synced_replicas : ",synced_replicas)
    #         if synced_replicas==no_of_awaited_replicas:
    #             print("synced_replicas : ",synced_replicas)
    #             return no_of_awaited_replicas
    #         else:
    #             await asyncio.sleep(0.001)  
    # print("synced_replicas :timeout ",synced_replicas)       
    # return synced_replicas
 

                    
                    
     
    

async def command_handler(writer,client_addr,server_role,query_string,input_tokens):
    input_tokens=query_string.splitlines()
    print('>>>>inside command_handler<<<<<')
    print(input_tokens)
    no_of_elements=int(input_tokens[0].lstrip('*'))               
    data_list=[]

    if no_of_elements == 1:
        if input_tokens[2] == 'PING':
            response=f"+PONG\r\n"
            # writer.write(response)
            # await writer.drain()  
            
    elif no_of_elements > 1:
        for token in input_tokens:
            if len(token)>1 and token.startswith('*'):                
                continue
            if token.startswith('$') and token.strip() != '$':
                continue
            data_list.append(token.strip())
        if data_list[0] == 'ECHO':
            if len(data_list[1:] ) > 1:
                echo_data=" ".join(data_list[1:])
            else:
                echo_data = data_list[1]
            string_length=len(echo_data)
            response=f"${string_length}\r\n{echo_data}\r\n"  
            # writer.write(response.encode())
            # await writer.drain() 
            # print('###RESPONSE###')
            # print(response)
        elif data_list[0].lower() == 'subscribe':
            if client_addr not in channel_subscriptions:
                channel_subscriptions[client_addr] = set()
            channel_subscriptions[client_addr].add(data_list[1])
            channels=len(channel_subscriptions[client_addr])
            response=f'*3\r\n$9\r\nsubscribe\r\n${len(data_list[1])}\r\n{data_list[1]}\r\n:{channels}\r\n'
        elif data_list[0].lower() == 'publish':
            subscribers=0
            if channel_subscriptions:
                for channels in channel_subscriptions.values():
                    if data_list[1] in channels:
                        subscribers += 1
            response=f':{subscribers}\r\n'

        elif data_list[0] == 'KEYS': 
            if data_list[1] == '*':
                all_keys=RedisAsyncServer.data_store.keys()
                response=f'*{len(all_keys)}\r\n'    
                for k in all_keys:
                    response=response+f'${len(k)}\r\n{k}\r\n'
            else:
                pattern=data_list[1]
                k_count=0
                response=''
                import fnmatch
                for k in all_keys:
                    if fnmatch.fnmatch(k, pattern):
                        response=response+f'${len(k)}\r\n{k}\r\n'
                        k_count+=1
                response=f'*{k_count}\r\n'+response

        elif data_list[0] == 'CONFIG':
            if data_list[1] == 'GET':
                if data_list[2].lower() == 'dir':
                    rdb_dir=RedisAsyncServer.server.rdb_dir
                    response=f'*2\r\n$3\r\ndir\r\n${len(rdb_dir)}\r\n{rdb_dir}\r\n'
                elif data_list[2].lower() == 'dbfilename':
                    rdb_filename=RedisAsyncServer.server.rdb_filename
                    response=f'*2\r\n$10\r\ndbfilename\r\n${len(rdb_filename)}\r\n{rdb_filename}\r\n'

        elif data_list[0] == 'SET':
            print("Inside SET , query_string",query_string)
            key=data_list[1]
            val=data_list[2]
            data_type = await get_data_type(val)
            expiry =None
            if len(data_list) > 3:
                if data_list[3] == 'PX':
                    expiry = datetime.now(timezone.utc) + timedelta(milliseconds=int(data_list[4]))
                elif data_list[3] == 'EX' :
                    expiry = datetime.now(timezone.utc) + timedelta(seconds=int(data_list[4]))
                
            RedisAsyncServer.data_store[key] = RedisObject(data = val,exp=expiry,data_type=data_type) 
            print(f'{server_role} set the new value !!!!')
            response=f"+OK\r\n" 
            
            # writer.write(response.encode())
            # await writer.drain() 
        elif data_list[0].upper() == 'WAIT' :
            if RedisAsyncServer.role == 'master':
                no_of_awaited_replicas = int(data_list[1])
                wait_command_timeout = float(data_list[2])
                no_of_ack_replicas=0
                current_time = datetime.now(timezone.utc)
                timeout=current_time+timedelta(milliseconds=wait_command_timeout)
                no_of_ack_replicas=await get_ack_replicas(no_of_awaited_replicas,timeout,wait_command_timeout)
                print("sending response from WAIT :",no_of_ack_replicas)                
                response=f':{no_of_ack_replicas}\r\n'
                # writer.write(response.encode())
                # await writer.drain() 
        # elif data_list[0].upper()=='REPLCONF' and data_list[1].upper()=='ACK':
            # replica_offset = int(data_list[2])
            # if replica_offset == ReplicaList[writer][0] :
            #     ReplicaList[writer][1] =True
            # else:
            #     ReplicaList[writer][1] =False  
            # print("Processed REPLCONF ACK")  
            # response='REPLCONF ACK'
                          
        elif data_list[0] == 'INCR': 
            key =data_list[1]
            response=None
            if key in RedisAsyncServer.data_store:
                redis_obj=RedisAsyncServer.data_store[key]
                if redis_obj.data.isdigit():
                    new_val = str(int(redis_obj.data)+1)
                    redis_obj.data = new_val
                    response =f':{new_val}\r\n'
                else:
                    response="-ERR value is not an integer or out of range\r\n"
            else:
                RedisAsyncServer.data_store[key] = RedisObject(data = '1',data_type='string') 
                response =f':1\r\n'
            # writer.write(response.encode())
            # await writer.drain() 
            
        elif data_list[0] == 'GET': 
            key=data_list[1]
            if key in RedisAsyncServer.data_store.keys() :
                val=RedisAsyncServer.data_store[key].data
                val_length=len(val)
                response=f'${val_length}\r\n{val}\r\n'
                expiry=RedisAsyncServer.data_store[key].exp
                if expiry :
                    if expiry < datetime.now(timezone.utc) :
                        response = f"$-1\r\n"                       
                    
            else:
                response=f"$-1\r\n"
            # writer.write(response.encode())
            # await writer.drain()
            # print('###RESPONSE###')
            # print(response) 
        elif data_list[0] == 'LPUSH': 
            key=data_list[1] 
            new_data_list=data_list[2:]
            if key not in RedisAsyncServer.data_store.keys() :
                RedisAsyncServer.data_store[key] = RedisObject(data = [],data_type='list') 
            redis_obj=RedisAsyncServer.data_store.get(key) 
            n=len(redis_obj.data)+len(new_data_list)  
            if new_data_list:
                for new_data in new_data_list:
                    redis_obj.data.insert(0,new_data) 
            if redis_obj.blocked_clients and redis_obj.data:
                pass                    
            response=f':{n}\r\n'
            # writer.write(response.encode())
            # await writer.drain()                                                          
                
        elif data_list[0] == 'RPUSH': 
            key=data_list[1] 
            new_data_list=data_list[2:]
            if key not in RedisAsyncServer.data_store.keys() :
                RedisAsyncServer.data_store[key] = RedisObject(data = [],data_type='list') 
            redis_obj=RedisAsyncServer.data_store.get(key) 
            n=len(redis_obj.data)+len(new_data_list)                   
            if new_data_list:
                for new_data in new_data_list:
                    redis_obj.data.append(new_data) 
                
            if redis_obj.blocked_clients and redis_obj.data:
                pass
            response=f':{n}\r\n'
            # writer.write(response.encode())
            # await writer.drain()                    
        elif data_list[0] == 'LRANGE': 
            key=data_list[1] 
            start_index=int(data_list[2].strip())
            stop_index=int(data_list[3].strip())
            print(">>>start=",start_index,"stop= ",stop_index)                         
            result_list=None                   
            if key  in RedisAsyncServer.data_store.keys() :                        
                redis_obj=RedisAsyncServer.data_store.get(key) 
                existing_list=redis_obj.data
                n=len(existing_list) 
                if start_index < 0 and start_index < -n:
                    start_index = 0                            
                elif stop_index <0 and stop_index < -n:
                    stop_index =0                            
                if start_index <0 and stop_index<0:
                    start_index=n+start_index
                    stop_index=n+stop_index                                                      
                elif start_index >=0 and stop_index < 0:
                    stop_index = n+stop_index
                if start_index >= n or start_index > stop_index:
                    response=f'*0\r\n'
                elif stop_index >= n:
                    stop_index=n
                    result_list=existing_list[start_index:stop_index]
                else:
                    stop_index+=1
                    result_list=existing_list[start_index:stop_index]
            else:
                response=f'*0\r\n'
            if result_list is not None:
                length=len(result_list)
                response=f'*{length}\r\n'+'\r\n'.join(f'${len(item)}\r\n{item}' for item in result_list)+f'\r\n'
            
            # print('>>>RESPONSE>>>>') 
            # print(response)                    
            # writer.write(response.encode())
            # await writer.drain()
        elif data_list[0] == 'LLEN': 
            key=data_list[1] 
            length=0
            if key in RedisAsyncServer.data_store:
                redis_obj=RedisAsyncServer.data_store[key]
                length=len(redis_obj.data)
                response=f':{length}\r\n'                    
            else:
                response=f':0\r\n'
            # print('###RESPONSE###')
            # print(response)
            # writer.write(response.encode())
            # await writer.drain()
        elif data_list[0] == 'BLPOP': 
            key=data_list[1] 
            waits_for=float(data_list[2]) # in seconds, 0 for infinite
            if key in RedisAsyncServer.data_store:
                redis_obj=RedisAsyncServer.data_store[key]
                if redis_obj.data:
                    # if data is available , send it to client immediately
                    ele=redis_obj.data.pop(0)
                    length=len(ele)
                    response=f'*2\r\n${len(key)}\r\n{key}\r\n${length}\r\n{ele}\r\n' 
                    print('###RESPONSE TO BLPOP CLIENT###')
                    print(response)
                    # writer.write(response.encode())
                    # await writer.drain() 
                else:
                    # if data is not available add to blocked clients
                    if waits_for == 0:
                        expires_on= datetime.max.replace(tzinfo=timezone.utc) # set to infinite datetime
                    else:
                        expires_on=datetime.now(timezone.utc) + timedelta(seconds=waits_for)
                    
                    client_tuple=tuple((writer,expires_on,key))
                    redis_obj.blocked_clients.append(client_tuple)                              
                    print('###add to blocked clients###')
                    response = await get_blpop_response(client_tuple)
                                        
            else:
                    RedisAsyncServer.data_store[key] = RedisObject(data = [],data_type='list')                             
                    redis_obj=RedisAsyncServer.data_store.get(key) 
                    if waits_for == 0:
                        expires_on= datetime.max.replace(tzinfo=timezone.utc) # set to infinite datetime
                    else:
                        expires_on=datetime.now(timezone.utc) + timedelta(seconds=waits_for)
                    
                    client_tuple=tuple((writer,expires_on,key))
                    redis_obj.blocked_clients.append(client_tuple)  
                    response = await get_blpop_response(client_tuple)
                    # response=f'$-1\r\n'
                    # print('###RESPONSE###')
                    # print(response)
            if not writer.is_closing():
                print('###RESPONSE###')
                print(response)
                # writer.write(response.encode())
                # await writer.drain()
                # print("send response>>>>>>>>>>>>>>")
        elif data_list[0] == 'LPOP': 
            key=data_list[1] 
            length=0
            n=len(data_list)
            if key in RedisAsyncServer.data_store:
                redis_obj=RedisAsyncServer.data_store[key]
                
                if redis_obj.data:
                    if n <3 :
                        ele=redis_obj.data.pop(0)
                        length=len(ele)
                        response=f'${length}\r\n{ele}\r\n'  
                    elif n>2:
                        pop_count=int(data_list[2])
                        if pop_count > len(redis_obj.data):
                            pop_count=len(redis_obj.data)
                        popped_elements=[]
                        for i in range(pop_count):
                            popped_elements.append(redis_obj.data.pop(0))
                        length=len(popped_elements)
                        print(f'>>>Popping {pop_count} elements<<<')
                        print(popped_elements)
                        print(">>>remaining elements<<<<<")
                        print(redis_obj.data)
                        response=f'*{length}\r\n'+''.join([f'${len(ele)}\r\n{ele}\r\n' for ele in popped_elements])
                                                            
            if length == 0:
                response=f'$-1\r\n'
            # print('###RESPONSE###')
            # print(response)
            # writer.write(response.encode())
            # await writer.drain()
        elif data_list[0] == 'XADD': 
            key=data_list[1]
            stream_key = data_list[2]
            # XADD key 0-1 foo bar
            #<millisecondsTime>-<sequenceNumber>
            AddStream = True
            if stream_key == '*':
                stream_key=str(get_milliseconds_time()) +'-*'                            
            stream_key_parts = str(stream_key).split('-')
            if key in RedisAsyncServer.data_store.keys() :
                redis_obj=RedisAsyncServer.data_store.get(key)   
                last_key=redis_obj.last_key
                if stream_key_parts[1].strip() != "*" :                           
                    valid,message = await valid_stream_key(stream_key,last_key)
                    if valid:
                        redis_obj.last_key = stream_key 
                    else:
                        AddStream = False
                else:
                    new_stream_key = await get_new_stream_key(stream_key_parts[0].strip(),redis_obj)
                    stream_key = new_stream_key
                    
            else:
                stream_key_parts = str(stream_key).split('-')
                stream_key_millisecondsTime = int(stream_key_parts[0].strip())
                if stream_key_parts[1].strip() != "*" : 
                    stream_key_sequenceNumber = int(stream_key_parts[1].strip())
                else:
                    if stream_key_millisecondsTime == 0 :
                        stream_key = '0-1'
                        stream_key_sequenceNumber = 1
                    else:
                        stream_key = str(stream_key_millisecondsTime)+'-0'
                        stream_key_sequenceNumber = 0
                
                if stream_key_millisecondsTime == 0 and stream_key_sequenceNumber == 0:
                    message =f'-ERR The ID specified in XADD must be greater than 0-0\r\n'
                    AddStream = False
                else:
                    RedisAsyncServer.data_store[key] = RedisObject(data = [],data_type='stream') 
                    redis_obj=RedisAsyncServer.data_store.get(key)
                    redis_obj.last_key=stream_key
                
            if AddStream :
                new_stream_entry=StreamEntry(id=stream_key)
                stream_entry_data=data_list[3:]
                i=0
                l=[]
                while i<len(stream_entry_data):                        
                    l.append([stream_entry_data[i],stream_entry_data[i+1]])
                    i += 2
                new_stream_entry.add_entry(l)                       
            
                redis_obj.data.append(new_stream_entry)
                
                response=f'${len(stream_key)}\r\n{stream_key}\r\n'  
            else:
                response = message
            # writer.write(response.encode())
            # await writer.drain() 
        elif data_list[0] == 'XRANGE': 
            key=data_list[1] 
            response=''
            if key in RedisAsyncServer.data_store.keys():
                print(">>>>>DATA LIST <<<<<<<<") 
                print(data_list)
                start=data_list[2]
                stop=data_list[3]
                redis_obj = RedisAsyncServer.data_store[key]
                response=get_xrange_response(redis_obj,start,stop)
                print("start-end:::",start,stop)
                print(response)
            # writer.write(response.encode()) 
            # await writer.drain() 
        elif data_list[0] == 'XREAD': 
            if data_list[1].upper() == 'STREAMS':
                xread_list=data_list[2:]
                #keys of streams to be read from data_store
                no_of_keys=int(len(xread_list)//2)
                xread_dict={}
                for i in range(no_of_keys):
                    xread_dict[xread_list[i]] =xread_list[i+no_of_keys]
                if xread_dict:
                    response=f'*{no_of_keys}\r\n'
                for key,stream_key in xread_dict.items():
                    # key=data_list[2]
                    # stream_key = data_list[3]
                    # response=''
                    print(">>>>>xread Key found<<<<",key,stream_key)
                    key_response=''
                    redis_obj=None
                    if key in RedisAsyncServer.data_store.keys():                                
                        redis_obj = RedisAsyncServer.data_store[key]
                        key_response,_=get_xread_response(key,redis_obj,stream_key)
                        response = response + key_response                                
            elif data_list[1].upper() == 'BLOCK' and data_list[3].upper() == 'STREAMS':
                block_ms=float(data_list[2])
                key=data_list[4] 
                stream_key=data_list[5] 
                xread_stream_block=True
                if key in RedisAsyncServer.data_store:
                    response=f'*1\r\n'
                    redis_obj =RedisAsyncServer.data_store[key]
                    if stream_key != '$':
                        key_response,data_len=get_xread_response(key,redis_obj,stream_key)
                        if data_len > 0:
                            print('$$$$$$data_list[2]=',data_list[2],' ; got response =',key_response)
                            response = response + key_response 
                            xread_stream_block=False
                    else:
                        stream_key=redis_obj.last_key
                if xread_stream_block:
                    #push to waiting queue and wait
                    if data_list[2].strip() == '0':
                        print("####INFINITE WAIT#####",client_addr)
                        expires_on= datetime.max.replace(tzinfo=timezone.utc) # set to infinite datetime
                    else:
                        expires_on = datetime.now(timezone.utc)+timedelta(milliseconds=block_ms)
                    xread_stream_block_que[key].append(client_addr)
                    block_response,block_expired=await xread_stream_block_handler(key,stream_key,expires_on,client_addr)
                    if not block_expired:
                        response=f'*1\r\n'+block_response
                    else:
                        response=block_response               

                                
            print(">>>>RESPONSE<<<<<")
            print(response)
            # writer.write(response.encode()) 
            # await writer.drain() 

        elif data_list[0] == 'TYPE': 
            key=data_list[1]
            if key in RedisAsyncServer.data_store.keys() :
                data_type= RedisAsyncServer.data_store.get(key).data_type
                response=f'+{data_type}\r\n'
            else:
                response=f'+none\r\n'
            # writer.write(response.encode())
            # await writer.drain()  
    return response                                         
    

async def client_handler(reader,writer):
    try:
        client_addr = writer.get_extra_info('peername')       
        
        print("Connected...",client_addr,RedisAsyncServer.role) 
        CONNECT = True
        
        # multi command enabled, queue to hold upcoming commands
        MULTI=[False,deque()]
        while CONNECT:
            input_query=await reader.read(1024)
            if not input_query:
                await asyncio.sleep(0.2)
                continue
            query_string=str(input_query.decode()) 
            print(">>>>>>>query_string : ",query_string)
            if query_string == '*2\r\n$4\r\nKEYS\r\n$3\r\n"*"\r\n':
                if RedisAsyncServer.data_store:
                    response=f'*{len(RedisAsyncServer.data_store)}\r\n'
                    for key in RedisAsyncServer.data_store.keys():
                        response=response+f'${len(key)}\r\n{key}\r\n'
                writer.write(response.encode())
                await writer.drain() 
                continue 
            # print('RECEIVED = ',query_string)
            input_tokens=query_string.splitlines()
            if client_addr in channel_subscriptions :
                if len(channel_subscriptions[client_addr])>0 :
                    allowed_cmds=['SUBSCRIBE','UNSUBSCRIBE','PSUBSCRIBE','PUNSUBSCRIBE','PING','QUIT']            
                    if input_tokens[2].upper() not in allowed_cmds:
                        response=f"-ERR Can't execute '{input_tokens[2]}' in subscribed mode\r\n"
                        writer.write(response.encode())
                        await writer.drain() 
                        continue  
                    elif input_tokens[2].upper()=='PING':
                        response=b'*2\r\n$4\r\npong\r\n$0\r\n\r\n' 
                        writer.write(response)
                        await writer.drain() 
                        continue                       
            
            if 'info' in input_tokens or 'INFO' in input_tokens:
                if 'replication' in input_tokens:
                    role=RedisAsyncServer.role
                    
                    length=5+len(role)
                    
                    if role == 'master' :
                        sec2='master_replid:'+RedisAsyncServer.server.master_replid
                        print('sec2 =',sec2)
                        sec3='master_repl_offset:'+str(RedisAsyncServer.server.master_repl_offset)
                        print('sec3 =',sec3)
                        master_resp=f'role:{role}\r\n{sec2}\r\n{sec3}\r\n'
                        response = f'${len(master_resp)}\r\n' + master_resp + f'\r\n'
                    else:
                        response=f'${length}\r\nrole:{role}\r\n'
                    print("RESPONSE = ", response)
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            if input_tokens[2].upper() == 'MULTI' : 
                MULTI[0]  = True
                response =b'+OK\r\n'
                writer.write(response)
                await writer.drain() 
                continue 
            if MULTI[0] :
                print('status = multi enabled')                
                if input_tokens[2].strip().upper() == 'EXEC':
                    if len(MULTI[1]) ==0 :
                        print('status = multi enabled,no queued command, got EXEC')
                        response=f'*0\r\n'
                        MULTI[0] = False
                        writer.write(response.encode())
                        await writer.drain()
                        continue
                    else:
                        que_length=len(MULTI[1])
                        response =f'*{que_length}\r\n'
                        while len(MULTI[1]) > 0 :
                            
                            query_string=MULTI[1].popleft()
                            input_tokens=query_string.splitlines()
                            cmd_response = await command_handler(writer,client_addr,RedisAsyncServer.role,query_string,input_tokens)
                            response = response+ f'{cmd_response}'
                        writer.write(response.encode())
                        print("$$$$$$RESPONSE::::",response)
                        await writer.drain() 
                        MULTI[0]=False
                        continue

                else:
                    if input_tokens[2].upper() == 'DISCARD' : 
                        MULTI[0]=False
                        MULTI[1]=deque()
                        response=f"+OK\r\n"
                    else:
                        print('status = multi enabled,not EXEC')
                        MULTI[1].append(query_string)
                        response=f"+QUEUED\r\n"
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            else:
                if input_tokens[2].upper() == 'EXEC' :                
                    response=b'-ERR EXEC without MULTI\r\n' 
                    writer.write(response)
                    await writer.drain() 
                    continue 
                if input_tokens[2].upper() == 'DISCARD' :                
                    response=b'-ERR DISCARD without MULTI\r\n' 
                    writer.write(response)
                    await writer.drain() 
                    continue  
            if 'REPLCONF' in input_tokens and 'ACK' in input_tokens and RedisAsyncServer.role == 'master':
                replica_offset = int(input_tokens[6]) + len(b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n')
                
                offset_by_master=RedisAsyncServer.server.ReplicaList[writer][0] 
                # print("offset by replica =",replica_offset)
                # print("offset by master =",offset_by_master)
                if replica_offset == offset_by_master:
                    print("!!! one replica offset matched !!!!")
                    RedisAsyncServer.server.ReplicaList[writer][1] =True
                continue 
            elif 'REPLCONF'  in input_tokens:
                if RedisAsyncServer.role == 'master':
                    response=f"+OK\r\n"
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            if 'PSYNC' in input_tokens:
                if RedisAsyncServer.role == 'master':
                    response=f'+FULLRESYNC {RedisAsyncServer.server.master_replid} 0\r\n'
                    writer.write(response.encode())
                    await writer.drain() 
                    rdb_hex='524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
                    #hex to bytes
                    in_bytes=bytes.fromhex(rdb_hex)
                    response2=f'${len(in_bytes)}\r\n'
                    writer.write(response2.encode())
                    writer.write(in_bytes)
                    await writer.drain()
                    #adding replica and its command offset
                    
                    RedisAsyncServer.server.ReplicaList[writer]=[0,True]
                    
                    print("$$$$$$ server ADDED to ReplicaList and set offet to 0$$$$$")
                    continue 
            
            # if 'WAIT' in input_tokens or 'wait' in input_tokens:
            #     if RedisAsyncServer.role == 'master':
            #         response=f':{len(ReplicaList)}\r\n'
            #         writer.write(response.encode())
            #         await writer.drain() 
            #     continue    
            if not query_string.startswith("*"):
                await asyncio.sleep(0.2)
                continue
            print("Calling command handler main")
            try:
                response = await command_handler(writer,client_addr,RedisAsyncServer.role,query_string,input_tokens)
            except Exception as e:
                print("EXCEPTION=",e)
            if input_tokens[2] == 'SET':
                new_cmd_str=''
                new_cmd_str='\r\n'.join(input_tokens)+'\r\n'
                if RedisAsyncServer.role == 'master' :
                    CommandDeque.append(new_cmd_str)
                    await propagate_command()
                    query_string=''
                    # await RedisAsyncServer.server.save()                    
            if not response == 'REPLCONF ACK':
                print("response from client handler")
                print("for command :",input_tokens)
                writer.write(response.encode())
                await writer.drain() 
            if not CONNECT:
                break
                
    except Exception as e:
        print("Client handling failed : Error ->",str(e))
    writer.close()
    await writer.wait_closed()

async def command_propagation_handler():
    print('inside command_propagation_handler',)
    command_offset=0
    try:
        m_reader,m_writer=await asyncio.open_connection(RedisAsyncServer.master_host,RedisAsyncServer.master_port)
        print(f"Connected to master {RedisAsyncServer.master_host}:{RedisAsyncServer.master_port}")
        #sending PING
        m_writer.write(b'*1\r\n$4\r\nPING\r\n')
        await m_writer.drain()
        data = await m_reader.readline()
        print("MASTER says:", data.decode())

        #sending REPLCONF listening-port <PORT>
        response=f'*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{RedisAsyncServer.port}\r\n'
        m_writer.write(response.encode())
        await m_writer.drain()
        data = await m_reader.readline()
        print("MASTER says:", data.decode())

        # REPLCONF capa psync2
        m_writer.write(b'*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n')
        await m_writer.drain()
        data = await m_reader.readline()
        if  'OK' in data.decode():
            # sending PSYNC to master
            response=f'*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n'
            m_writer.write(response.encode())
            await m_writer.drain()
            data = await m_reader.readline()
            print("MASTER says:", data.decode())
            data = await m_reader.readline()
            print("MASTER says:", data.decode())
            length=int(data.decode().splitlines()[0].lstrip('$'))
            rdbfile=await m_reader.readexactly(length) 
            print(f"MASTER send RDB file of {length} bytes")
            
        while True:
            try:
                print("Inside while loop, waiting on master command !!!!")
                command=await m_reader.read(1024)
                command_len=0
                print("COMMAND=",command) 
                if not command:
                    await asyncio.sleep(0.1)
                    continue
                
                query_string=str(command.decode()) 
                if command == b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n' or command == b'*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n' :
                    print('gectack found !!!!!command_offset is ',command_offset)
                    response=f'*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(command_offset))}\r\n{str(command_offset)}\r\n'  
                    m_writer.write(response.encode())  
                    await m_writer.drain() 
                    print('offset send to master for getack')
                    command_len=len(command)
                    command_offset=command_offset + command_len
                    RedisAsyncServer.server.replica_command_offset = command_offset
                    print("slave adding getack offset",command_offset)
                    command_len=0
                    continue     
                input_tokens=query_string.splitlines()
                data_lists=deque()
                length_list=[]
                print("query_string=",query_string)        
                for token in input_tokens:
                    if len(token)>1 and token.startswith('*'):   
                        length_list.append(int(token.lstrip('*')))             
                        continue
                    if token.startswith('$') and token.strip() != '$':
                        continue
                    if token.strip() == '+OK' :
                        continue
                    data_lists.append(token.strip())
                print("data_list:",data_lists)
                commands_list=[]
                for l in length_list:
                    new_com=[]
                    i=1
                    while i<=l:
                        new_com.append(data_lists.popleft())
                        i=i+1
                    commands_list.append(new_com)

                for data_list in commands_list: 
                    print("processing command:",data_list)               
                    if data_list[0] == 'SET':
                        command_len=0
                        print("Inside SET ")
                        key=data_list[1]
                        val=data_list[2]                        
                        data_type = await get_data_type(val)
                        expiry =None
                        if len(data_list) > 3:
                            if data_list[3] == 'PX':
                                expiry = datetime.now(timezone.utc) + timedelta(milliseconds=int(data_list[4]))
                            elif data_list[3] == 'EX' :
                                expiry = datetime.now(timezone.utc) + timedelta(seconds=int(data_list[4]))
                            
                        RedisAsyncServer.data_store[key] = RedisObject(data = val,exp=expiry,data_type=data_type) 
                        command_string=''

                        for s in data_list:
                            command_string=command_string+f'${len(str(s))}\r\n{str(s)}\r\n'

                        command_string=f'*{len(data_list)}\r\n'+command_string
                        command_len=len(command_string.encode())
                        print("SET string",command_string)
                        print("SET offset",command_len)
                        command_offset=command_offset+command_len
                        RedisAsyncServer.server.replica_command_offset = command_offset
                        print("slave adding SET offset",command_offset)
                        command_len=0
                        print(f'slave set the new value !!!!',RedisAsyncServer.data_store[key].data)
                    elif data_list[0].upper() == 'REPLCONF' and data_list[1].upper() == 'GETACK': 
                        response=f'*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(RedisAsyncServer.server.replica_command_offset))}\r\n{str(RedisAsyncServer.server.replica_command_offset)}\r\n'  
                        m_writer.write(response.encode())  
                        await m_writer.drain() 
                        command_len=len(b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n')
                        command_offset=command_offset + command_len
                        RedisAsyncServer.server.replica_command_offset = command_offset
                        print("slave adding offset getack",command_offset)
                        command_len=0
                        continue     
                    elif data_list[0].upper() == 'PING' :
                        # response=f"+PONG\r\n"
                        # m_writer.write(response.encode())  
                        # await m_writer.drain() 
                        command_len=14
                        command_offset=command_offset + command_len
                        RedisAsyncServer.server.replica_command_offset = command_offset
                        print("slave adding PING offset",command_offset)
                        command_len=0
                        continue     

                    elif data_list[0] == 'INCR': 
                        key =data_list[1]
                        response=None
                        if key in RedisAsyncServer.data_store:
                            redis_obj=RedisAsyncServer.data_store[key]
                            if redis_obj.data.isdigit():
                                new_val = str(int(redis_obj.data)+1)
                                redis_obj.data = new_val
                                # response =f':{new_val}\r\n'
                            else:
                                pass
                                # response="-ERR value is not an integer or out of range\r\n"
                        else:
                            RedisAsyncServer.data_store[key] = RedisObject(data = '1',data_type='string') 
                # command_offset=command_offset + command_len
            except Exception as e:
                print("EXCEPTION=",e)
    except Exception as e:
            print("Client handling failed : Error ->",str(e))
    finally:
        m_writer.close()
        await m_writer.wait_closed()

                                        
                        
           

async def run_server(port_number):
    try:
        redis_server=await asyncio.start_server(client_handler,host="localhost",port=port_number)
        print(f'Redis server listening {redis_server.sockets[0].getsockname()}')
        RedisAsyncServer.port =port_number
        if RedisAsyncServer.role=='slave':
            asyncio.create_task(command_propagation_handler())            
        asyncio.create_task(blocked_client_handler())                    
        await redis_server.serve_forever()
    except Exception as e:
        print("Server execution failed : Error ->",str(e))

import sys
def main():
    master_details=''
    port_number=6379
    if '--port' in sys.argv:
        try:
            args=sys.argv
            port_number=int(args[args.index('--port')+1])
        except:
            port_number=6379
    else:
        port_number=6379
    if '--replicaof' in sys.argv:
        try:
            RedisAsyncServer.role='slave'
            RedisAsyncServer.server=Replica()
            args=sys.argv
            master_details=args[args.index('--replicaof')+1].split(' ')
            master_host = master_details[0].strip()
            master_port = int(master_details[1].strip())
            RedisAsyncServer.master_host=master_host
            RedisAsyncServer.master_port=master_port
            
        except:
            pass
    #--dir /tmp/redis-files --dbfilename dump.rdb
    if '--dir' in sys.argv:
        args=sys.argv
        RDB_DIR=args[args.index('--dir')+1]
        RedisAsyncServer.server.rdb_dir=RDB_DIR
    if '--dir' in sys.argv:
        args=sys.argv
        RDB_FILENAME=args[args.index('--dbfilename')+1]
        RedisAsyncServer.server.rdb_filename=RDB_FILENAME
    if RedisAsyncServer.role=='master':
        RedisAsyncServer.server.master_replid = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        RedisAsyncServer.server.master_repl_offset = 0
        if RedisAsyncServer.server.rdb_dir and RedisAsyncServer.server.rdb_filename:
            initialize_data_store()
            
    print("Execution starts here....!role=",RedisAsyncServer.role, master_details)

    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # conn, _ =server_socket.accept() # wait for client        
    # while True :
    #     data = conn.recv(1024)
    #     conn.sendall(b"+PONG\r\n")

    asyncio.run(run_server(port_number))
    


if __name__ == "__main__":
    main()
