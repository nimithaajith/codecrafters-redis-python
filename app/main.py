import asyncio
from datetime import timezone,timedelta,datetime
from collections import deque,defaultdict
from . import geo_encode,hashing,utilities
from .data_models import RedisObject,Transaction
from .utilities import get_xrange_response,get_xread_response,setup_server
from . import get_commands
from . import set_commands

#globals
RedisAsyncServer=None
channel_subscriptions={}
CommandDeque=deque()
xread_stream_block_que=defaultdict(list)
transaction_lock=Transaction()

    

async def blocked_client_handler():
    global RedisAsyncServer
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
                            except:
                                pass 
                    except:
                        pass  
        await asyncio.sleep(0.5)   # check twice per second


                    
async def get_blpop_response(client_tuple) :
    global RedisAsyncServer
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


        

def get_data_type(val):
    if isinstance(val,str):
        return 'string'
    else:
        None


async def xread_stream_block_handler(key,stream_key,expires_on,client_addr):
    global RedisAsyncServer
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
    global RedisAsyncServer
    while len(CommandDeque)>0:
        command_str=CommandDeque.popleft()
        # >>>PROPAGATING>>>
        cmd_encoded=command_str.encode()
        for s_writer in RedisAsyncServer.ReplicaList.keys():             
            s_writer.write(cmd_encoded)
            await s_writer.drain()            
            curr_offset=RedisAsyncServer.ReplicaList[s_writer][0]
            RedisAsyncServer.ReplicaList[s_writer][0] = curr_offset+len(cmd_encoded)
            RedisAsyncServer.ReplicaList[s_writer][1]=False
            print("Updated offset and sync to false by master")
            # await asyncio.sleep(0.001)

async def process_synced_replicas(synced_replicas,replica_temp_list,no_of_awaited_replicas):
    global RedisAsyncServer
    for s_writer in RedisAsyncServer.ReplicaList.keys():
        if s_writer in replica_temp_list:
            if RedisAsyncServer.ReplicaList[s_writer][1]:
                synced_replicas += 1
                replica_temp_list.remove(s_writer)
    return synced_replicas,replica_temp_list

def check_and_update_locks(modified_data):
    m_key,this_client=modified_data
    for c_adrs in transaction_lock.locks:
        if c_adrs != this_client :
            locked_key_data=transaction_lock.locks[c_adrs]
            i=0
            l=len(locked_key_data)
            while i<l:
                key,state=locked_key_data[i]            
                if key == m_key:
                    if not state:
                        transaction_lock.locks[c_adrs][i] = [key,True]
                        print('======================LOCK UPDATE===================================')
                        print(f'{this_client} client modified key {key} locked by client {c_adrs}')
                        break
                i=i+1            



async def propagate_getack_command(replica_temp_list):
    global RedisAsyncServer
    cmd_encoded= b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'
    for s_writer in RedisAsyncServer.ReplicaList.keys():
        if s_writer in replica_temp_list:                
            s_writer.write(cmd_encoded)
            await s_writer.drain()            
            curr_offset=RedisAsyncServer.ReplicaList[s_writer][0]
            # server side update of offset
            RedisAsyncServer.ReplicaList[s_writer][0] = curr_offset+len(cmd_encoded)
            RedisAsyncServer.ReplicaList[s_writer][1] = False
    

            
async def get_ack_replicas(no_of_awaited_replicas,timeout,waittime):
    global RedisAsyncServer
    replica_temp_list=list(RedisAsyncServer.ReplicaList.keys()) 
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
                      
    

async def command_handler(writer,client_addr,server_role,query_string,data_list):
    global RedisAsyncServer
    global channel_subscriptions
    response=''
    print('>>>>inside command_handler<<<<<')    
    modified_key=[]    
    if data_list[0].upper() == 'PING':
        response=f"+PONG\r\n"     
    elif data_list[0].upper() == 'ECHO':
        if len(data_list[1:] ) > 1:
            echo_data=" ".join(data_list[1:])
        else:
            echo_data = data_list[1]
        string_length=len(echo_data)
        response=f"${string_length}\r\n{echo_data}\r\n"  
        
    elif data_list[0].upper() == 'SUBSCRIBE':
       
        channel_subscriptions=set_commands.subscribe(data_list,writer,client_addr,channel_subscriptions)
        channels=len(channel_subscriptions[client_addr][0])
        response=f'*3\r\n$9\r\nsubscribe\r\n${len(data_list[1])}\r\n{data_list[1]}\r\n:{channels}\r\n'
    elif data_list[0].upper() == 'UNSUBSCRIBE':
        
        channel_subscriptions=set_commands.unsubscribe(data_list,client_addr,channel_subscriptions)               
        channels=len(channel_subscriptions[client_addr][0])
        response=f'*3\r\n$11\r\nunsubscribe\r\n${len(data_list[1])}\r\n{data_list[1]}\r\n:{channels}\r\n'    

    elif data_list[0].upper() == 'PUBLISH':
        subscribers= await set_commands.publish_message(data_list,channel_subscriptions)        
        response=f':{subscribers}\r\n'
    elif data_list[0] == 'KEYS': 
        response= get_commands.get_keys(data_list,RedisAsyncServer.data_store)        

    elif data_list[0] == 'CONFIG':
        if data_list[1] == 'GET':
            response= get_commands.config_get(data_list,RedisAsyncServer)            
    elif data_list[0] == 'SET':     
        key=data_list[1]   
        RedisAsyncServer.data_store=set_commands.set_key(data_list,RedisAsyncServer.data_store)         
        modified_key=[key,client_addr]
        print(f'{server_role} set the new value !!!!')
        if server_role == 'master' :
            if RedisAsyncServer.appendfsync == 'always':
                utilities.add_command(query_string,RedisAsyncServer)                
        response=f"+OK\r\n"        
        
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
                        
    elif data_list[0].upper() == 'INCR': 
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
            modified_key=[key,client_addr]
            response =f':1\r\n'
        
    elif data_list[0].upper() == 'GET': 
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
        
    elif data_list[0].upper() == 'LPUSH': 
        key=data_list[1] 
        new_data_list=data_list[2:]
        if key not in RedisAsyncServer.data_store.keys() :
            RedisAsyncServer.data_store[key] = RedisObject(data = [],data_type='list') 
        redis_obj=RedisAsyncServer.data_store.get(key) 
        n=len(redis_obj.data)+len(new_data_list)  
        if new_data_list:
            for new_data in new_data_list:
                redis_obj.data.insert(0,new_data) 
            modified_key=[key,client_addr]
        if redis_obj.blocked_clients and redis_obj.data:
            pass                    
        response=f':{n}\r\n'                                                                   
            
    elif data_list[0].upper() == 'RPUSH': 
        key=data_list[1] 
        new_data_list=data_list[2:]
        if key not in RedisAsyncServer.data_store.keys() :
            RedisAsyncServer.data_store[key] = RedisObject(data = [],data_type='list') 
        redis_obj=RedisAsyncServer.data_store.get(key) 
        n=len(redis_obj.data)+len(new_data_list)                   
        if new_data_list:
            for new_data in new_data_list:
                redis_obj.data.append(new_data) 
            modified_key=[key,client_addr]            
        if redis_obj.blocked_clients and redis_obj.data:
            pass
        response=f':{n}\r\n'
                            
    elif data_list[0].upper() == 'LRANGE': 
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
        
        
    elif data_list[0].upper() == 'LLEN': 
        key=data_list[1] 
        length=0
        if key in RedisAsyncServer.data_store:
            redis_obj=RedisAsyncServer.data_store[key]
            length=len(redis_obj.data)
            response=f':{length}\r\n'                    
        else:
            response=f':0\r\n'
        
    elif data_list[0].upper() == 'BLPOP': 
        key=data_list[1] 
        waits_for=float(data_list[2]) # in seconds, 0 for infinite
        if key in RedisAsyncServer.data_store:
            redis_obj=RedisAsyncServer.data_store[key]
            if redis_obj.data:
                # if data is available , send it to client immediately
                ele=redis_obj.data.pop(0)
                modified_key=[key,client_addr]
                length=len(ele)
                response=f'*2\r\n${len(key)}\r\n{key}\r\n${length}\r\n{ele}\r\n' 
                print('###RESPONSE TO BLPOP CLIENT###')
                print(response)
                
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
                
        if not writer.is_closing():
            print('###RESPONSE###')
            print(response)
            
    elif data_list[0].upper() == 'LPOP': 
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
                    #Popping {pop_count} elements
                    for i in range(pop_count):
                        popped_elements.append(redis_obj.data.pop(0))
                    length=len(popped_elements)                        
                    response=f'*{length}\r\n'+''.join([f'${len(ele)}\r\n{ele}\r\n' for ele in popped_elements])                                                            
        if length == 0:
            response=f'$-1\r\n'
        else:
            modified_key=[key,client_addr]
        
    elif data_list[0].upper() == 'XADD': 
        
        RedisAsyncServer.data_store,response,AddStream=await set_commands.xadd(data_list,RedisAsyncServer.data_store)
        if AddStream:
            key=data_list[1]
            modified_key=[key,client_addr]
    elif data_list[0].upper() == 'XRANGE': 
        key=data_list[1] 
        response=''
        if key in RedisAsyncServer.data_store.keys():            
            start=data_list[2]
            stop=data_list[3]
            redis_obj = RedisAsyncServer.data_store[key]
            response=get_xrange_response(redis_obj,start,stop)
                    
    elif data_list[0].upper() == 'XREAD': 
        if data_list[1].upper() == 'STREAMS':
            response= get_commands.xread_streams(data_list,RedisAsyncServer.data_store)
                                           
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
    elif data_list[0].upper() == 'ZADD':
        # ZADD racer_scores 8.0 "Sam"
        print('data_list =', data_list)
        key=data_list[1]
        score=float(data_list[2])
        member = data_list[3]
        response=None
        score_list=list((score,member))            
        if key not in RedisAsyncServer.data_store:
            new_redis_object = RedisObject(data=[],data_type='sortedset')
            updated_data=[]
            updated_data.append(score_list)                
            RedisAsyncServer.data_store[key] = new_redis_object            
        else:
            memberExists = False
            old_data = RedisAsyncServer.data_store[key].data
            for l in old_data:
                if l[1] == member :
                    updated_data = old_data 
                    updated_data.remove(l)
                    updated_data.append(score_list)                        
                    response=':0\r\n' 
                    memberExists = True                      
                    break
            if not memberExists:
                updated_data=RedisAsyncServer.data_store[key].data 
                updated_data.append(score_list)            
        new_sorted_data  = sorted(updated_data,key=lambda x : (x[0],x[1])) 
        print("new data = ",new_sorted_data)
        RedisAsyncServer.data_store[key].data =new_sorted_data  
        modified_key=[key,client_addr]
        if response is None:
            response=':1\r\n' 
    elif data_list[0].upper() == 'ZRANK':
        # ZRANK zset_key member
        response=get_commands.get_zrank(data_list,RedisAsyncServer.data_store)           
        
    elif data_list[0].upper() == 'ZRANGE' :
        # ZRANGE racer_scores 0 2
        response=get_commands.get_zrange(data_list,RedisAsyncServer.data_store)
        
        print("zrange response = ",response)
    elif data_list[0].upper() == 'ZCARD' :
        # ZCARD zset_key
        response=get_commands.get_zcard(data_list,RedisAsyncServer.data_store)
            
    elif data_list[0].upper() == 'ZSCORE' :
        # ZSCORE zset_key member
        response = get_commands.get_zscore(data_list,RedisAsyncServer.data_store)
        
        print("zscore response = ",response)
    elif data_list[0].upper() == 'ZREM' :
        # ZREM zset_key member
        key = data_list[1]
        member=data_list[2]
        if key not in RedisAsyncServer.data_store :
            response = ":0\r\n" 
        else:
            is_member = False
            old_data = RedisAsyncServer.data_store[key].data
            for l in old_data:
                if l[1] == member : 
                    old_data.remove(l) 
                    RedisAsyncServer.data_store[key].data =old_data  
                    modified_key=[key,client_addr]                                                                
                    response=':1\r\n' 
                    is_member = True                      
                    break
            if not is_member:
                response = ':0\r\n'
    elif data_list[0].upper() == 'GEOADD':
    #GEOADD key longitude latitude placename  
        key=data_list[1]
        longitude=float(data_list[2])
        latitude=float(data_list[3]) 
        placename=data_list[4]
        response=None
        if longitude < -180 or longitude > 180 or latitude < -85.05112878 or latitude > 85.05112878 :
            response=f'-ERR invalid longitude,latitude pair {longitude},{latitude}\r\n'
        else:            
            geoscore= geo_encode.encode(latitude=latitude,longitude=longitude)
            score_list=list((geoscore,placename))            
            if key not in RedisAsyncServer.data_store:
                new_redis_object = RedisObject(data=[],data_type='sortedset')
                updated_data=[]
                updated_data.append(score_list)                
                RedisAsyncServer.data_store[key] = new_redis_object                
            else:
                memberExists = False
                old_data = RedisAsyncServer.data_store[key].data
                for l in old_data:
                    if l[1] == placename :
                        updated_data = old_data 
                        updated_data.remove(l)
                        updated_data.append(score_list)                        
                        response=':0\r\n' 
                        memberExists = True                      
                        break
                if not memberExists:
                    updated_data=RedisAsyncServer.data_store[key].data 
                    updated_data.append(score_list)
            
            new_sorted_data  = sorted(updated_data,key=lambda x : (x[0],x[1])) 
            print("new data = ",new_sorted_data)
            RedisAsyncServer.data_store[key].data =new_sorted_data  
            modified_key=[key,client_addr]
            if response is None:
                response=':1\r\n' 

    elif data_list[0].upper() == 'GEOPOS' :
        # GEOPOS key member1 member2
        response = get_commands.geo_position(data_list,RedisAsyncServer.data_store)
                    

    elif data_list[0].upper() == 'GEODIST' :
        # GEODIST places Munich Paris  , 
        response = get_commands.geo_dist(data_list,RedisAsyncServer.data_store)            
        
    elif data_list[0].upper() == 'GEOSEARCH' :
        # GEOSEARCH key FROMLONLAT longitude latitude BYRADIUS radius unit(m) 
        response =  get_commands.geo_search(data_list,RedisAsyncServer.data_store)           

    elif data_list[0].upper() == 'ACL':
        current_users=RedisAsyncServer.clients
        if data_list[1].upper() == 'WHOAMI':            
            response = '$7\r\ndefault\r\n'
        elif data_list[1].upper() == 'GETUSER':
            response = get_commands.get_user(data_list,current_users)            
        elif data_list[1].upper() == 'SETUSER':
            # ACL SETUSER default >mypassword
            user_name=data_list[2]
            if data_list[3].startswith('>'):
                raw_password=str(data_list[3]).lstrip('>')                
                for user in current_users:
                    if user.username == user_name:
                        RedisAsyncServer.clients.remove(user)
                        user.password = hashing.hash_password(raw_password)
                        flags=user.flags
                        if 'nopass' in flags:
                            user.flags.remove('nopass') 
                        RedisAsyncServer.clients.append(user) 
                        break   
            response='+OK\r\n'
    elif data_list[0].upper() == 'AUTH':
        #AUTH <username> <password>
        user_name=data_list[1]
        user_pass=data_list[2]
        response='-WRONGPASS invalid username-password pair or user is disabled.\r\n'            
        for user in RedisAsyncServer.clients:
            if user.username == user_name:
                if user.password == hashing.hash_password(str(user_pass)):
                    response='+OK\r\n'
                    if not utilities.client_exists(RedisAsyncServer.clients,client_addr):
                        RedisAsyncServer.clients=utilities.add_client(user_name,RedisAsyncServer.clients,client_addr)
                    break                    

    elif data_list[0] == 'TYPE': 
        response =  get_commands.get_type(data_list,RedisAsyncServer.data_store)         
        
    if modified_key:
        print("calling check and update lock")
        check_and_update_locks(modified_key) 
    return response   

async def get_command(cli_reader):
    query_string=''
    data_list=[]
    line1=await cli_reader.readline()            
    if not line1 or not line1.startswith(b'*'):
        return query_string,data_list 
    cmd_length=int(line1.decode()[1:-2])
    if cmd_length:        
        query_string=f'{line1.decode()[:-2]}\r\n'
        print('query_string= ',query_string)
        data_list=[]
        for _ in range(cmd_length):
            part=await cli_reader.readline()
            if part and part.startswith(b'$'): 
                query_string=query_string+f'{part.decode()[:-2]}\r\n'
                str_len=int(part.decode()[1:-2])
                encoded_ip_str=await cli_reader.readexactly(str_len + 2)
                ip_str=encoded_ip_str.decode()[:-2]
                data_list.append(ip_str)                
                query_string=query_string+f'{ip_str}\r\n'
            else:
                return '',[]  
        return query_string,data_list      
    else:
        return '',[]  
                            
    

async def client_handler(reader,writer):
    try:
        global RedisAsyncServer
        client_addr = writer.get_extra_info('peername')  
        userobjs=RedisAsyncServer.clients  
        print('current users=',userobjs)   
        if len(userobjs) ==1 :
            default_client=userobjs[0]
            print("current user = ",default_client.username,type(default_client))
            if default_client.username == 'default' :
                if not default_client.client_address:
                    default_client.client_address.append(client_addr)
                    RedisAsyncServer.clients[0] = default_client                
        
        print("Connected...",client_addr,RedisAsyncServer.role) 
        CONNECT = True        
        # multi command enabled, queue to hold upcoming commands
        transaction_lock.locks[client_addr]=[]
        MULTI=[False,deque()]
        while CONNECT:
            ########Reading and parsing one command#########
            query_string,data_list=await get_command(reader)
            # line1=await reader.readline()            
            if not query_string or not data_list:
                await asyncio.sleep(0.2)
                continue
           
            
            print('#####PROCESSING QUERY #########')
            print(query_string) 
            print('##### DATA_LIST #########')
            print(data_list) 
            if not utilities.allow_commands(userobjs,client_addr):
                if 'AUTH' not in query_string.splitlines():
                    response='-NOAUTH Authentication required.\r\n'
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            
            # if query_string == '*2\r\n$4\r\nKEYS\r\n$3\r\n"*"\r\n':
            #     if RedisAsyncServer.data_store:
            #         response=f'*{len(RedisAsyncServer.data_store)}\r\n'
            #         for key in RedisAsyncServer.data_store.keys():
            #             response=response+f'${len(key)}\r\n{key}\r\n'
            #     writer.write(response.encode())
            #     await writer.drain() 
            #     continue 
            
            if client_addr in channel_subscriptions :
                if len(channel_subscriptions[client_addr])>0 :
                    allowed_cmds=['SUBSCRIBE','UNSUBSCRIBE','PSUBSCRIBE','PUNSUBSCRIBE','PING','QUIT']            
                    if data_list[0].upper() not in allowed_cmds:
                        response=f"-ERR Can't execute '{data_list[0]}' in subscribed mode\r\n"
                        writer.write(response.encode())
                        await writer.drain() 
                        continue  
                    elif data_list[0].upper()=='PING':
                        response=b'*2\r\n$4\r\npong\r\n$0\r\n\r\n' 
                        writer.write(response)
                        await writer.drain() 
                        continue                       
            
            if 'info' in data_list or 'INFO' in data_list:
                if 'replication' in data_list:
                    role=RedisAsyncServer.role                    
                    length=5+len(role)                    
                    if role == 'master' :
                        sec2='master_replid:'+RedisAsyncServer.master_replid
                        print('sec2 =',sec2)
                        sec3='master_repl_offset:'+str(RedisAsyncServer.master_repl_offset)
                        print('sec3 =',sec3)
                        master_resp=f'role:{role}\r\n{sec2}\r\n{sec3}\r\n'
                        response = f'${len(master_resp)}\r\n' + master_resp + f'\r\n'
                    else:
                        response=f'${length}\r\nrole:{role}\r\n'
                    print("RESPONSE = ", response)
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            if data_list[0].upper() == "UNWATCH":
                if client_addr in transaction_lock.locks:
                    transaction_lock.locks[client_addr].clear()
                respone=b'+OK\r\n'
                writer.write(respone)
                await writer.drain() 
                continue 
            if data_list[0].upper() == 'WATCH' : 
                if MULTI[0] :
                    response=b'-ERR WATCH inside MULTI is not allowed\r\n'
                    writer.write(response)
                    await writer.drain() 
                    continue                 
                watched_keys=[]                
                for watch_key in data_list[1:] :
                    watched_keys.append(watch_key)                    
                print("watched_keys =",watched_keys)
                #locks keep track of key and if modified or not state
                #state will be set as true if any other client modifies this key after this
                for watched_k in watched_keys:
                    keystatelst=[watched_k,False]
                    transaction_lock.locks[client_addr].append(keystatelst)
                response=b'+OK\r\n'
                writer.write(response)
                await writer.drain() 
                continue 
            if data_list[0].upper() == 'MULTI' : 
                if not MULTI[0]:
                    MULTI[0]  = True
                    response =b'+OK\r\n'
                    writer.write(response)
                    await writer.drain() 
                    continue 
            if MULTI[0] :
                print('status = multi enabled')                
                if data_list[0].strip().upper() == 'EXEC':
                    print('exec command ......')
                    if len(MULTI[1]) ==0 :
                        print('status = multi enabled,no queued command, got EXEC')
                        response=f'*0\r\n'
                        MULTI[0] = False
                        writer.write(response.encode())
                        await writer.drain()
                        # continue
                    else:
                        que_length=len(MULTI[1])
                        print('No of commands queued =',que_length)
                        Abort=False                                                              
                        print('ckecking for modified keys')                     
                        if client_addr in transaction_lock.locks:  
                            print(f"{client_addr} found...")                           
                            for k,s in transaction_lock.locks[client_addr]:
                                print(f"checking key ={k} ,state = {s}...")   
                                if s :
                                    print(f"{k} got modified......")
                                    Abort=True
                                    break
                                        
                        if Abort:
                            print("Aborting EXEC command..........")
                            MULTI[1].clear()
                            MULTI[0]=False
                            response=b'*-1\r\n'
                            writer.write(response)
                            print("Aborted, RESPONSE:",response)
                            await writer.drain() 
                            
                        else:
                            response =f'*{que_length}\r\n'
                            while len(MULTI[1]) > 0 :                            
                                query_string=MULTI[1].popleft()
                                input_tokens=query_string.splitlines() 
                                input_tokens_list=[]                                
                                cmd_parsed=True
                                cmd_length=0
                                if input_tokens[0] and input_tokens[0].startswith('*'):                                    
                                    cmd_length=int(input_tokens[0][1:])
                                else: 
                                    cmd_parsed=False
                                if cmd_length:                          
                                    i=1                                    
                                    while i < len(input_tokens):                                    
                                        part=input_tokens[i]
                                        if part and part.startswith('$'): 
                                            inp_str=input_tokens[i+1]
                                            input_tokens_list.append(inp_str) 
                                            i=i+2
                                        else:
                                            cmd_parsed=False
                                            break                                 
                                else:
                                    cmd_parsed=False
                                if cmd_parsed:
                                    cmd_response = await command_handler(writer,client_addr,RedisAsyncServer.role,query_string,input_tokens_list)
                                    response = response+ f'{cmd_response}'
                            writer.write(response.encode())
                            print("multi not aborted , RESPONSE =",response)
                            await writer.drain() 
                            MULTI[0]=False
                    transaction_lock.locks[client_addr].clear()        
                    continue
                else:
                    if data_list[0].upper() == 'DISCARD' : 
                        MULTI[0]=False
                        MULTI[1]=deque()
                        response=f"+OK\r\n"
                        transaction_lock.locks[client_addr].clear()
                    else:
                        print('status = multi enabled,not EXEC')
                        MULTI[1].append(query_string)
                        response=f"+QUEUED\r\n"
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            else:
                if data_list[0].upper() == 'EXEC' :                
                    response=b'-ERR EXEC without MULTI\r\n' 
                    writer.write(response)
                    await writer.drain() 
                    continue 
                if data_list[0].upper() == 'DISCARD' :                
                    response=b'-ERR DISCARD without MULTI\r\n' 
                    writer.write(response)
                    await writer.drain() 
                    transaction_lock.locks[client_addr].clear()
                    continue  
            if 'REPLCONF' in data_list and 'ACK' in data_list and RedisAsyncServer.role == 'master':
                replica_offset = int(data_list[2]) + len(b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n')                
                offset_by_master=RedisAsyncServer.ReplicaList[writer][0] 
                if replica_offset == offset_by_master:
                    print("!!! one replica offset matched !!!!")
                    RedisAsyncServer.ReplicaList[writer][1] =True
                continue 
            elif 'REPLCONF'  in data_list:
                if RedisAsyncServer.role == 'master':
                    response=f"+OK\r\n"
                    writer.write(response.encode())
                    await writer.drain() 
                    continue 
            if 'PSYNC' in data_list:
                if RedisAsyncServer.role == 'master':
                    response=f'+FULLRESYNC {RedisAsyncServer.master_replid} 0\r\n'
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
                    RedisAsyncServer.ReplicaList[writer]=[0,True]                    
                    print("$$$$$$ server ADDED to ReplicaList and set offet to 0$$$$$")
                    continue            
            print("Calling command handler main")
            try:
                response = await command_handler(writer,client_addr,RedisAsyncServer.role,query_string,data_list)
            except Exception as e:
                print("EXCEPTION=",e)
            if data_list[0].upper() == 'SET':
                if RedisAsyncServer.role == 'master' :
                    CommandDeque.append(query_string)
                    await propagate_command()
                    query_string=''                                       
            if not response == 'REPLCONF ACK':
                print("response got from client handler")                
                writer.write(response.encode())
                await writer.drain() 
            if not CONNECT:
                break                
    except Exception as e:
        print("Client handling failed : Error ->",str(e))
    writer.close()
    await writer.wait_closed()


async def command_propagation_handler():
    global RedisAsyncServer
    print('inside command_propagation_handler',)
    command_offset=0
    try:
        m_reader,m_writer=await asyncio.open_connection(RedisAsyncServer.master_host,RedisAsyncServer.master_port)
        print(f"Connected to master {RedisAsyncServer.master_host}:{RedisAsyncServer.master_port}")
        #sending PING
        m_writer.write(b'*1\r\n$4\r\nPING\r\n')
        await m_writer.drain()
        data = await m_reader.readline()
        #sending REPLCONF listening-port <PORT>
        response=f'*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{RedisAsyncServer.port}\r\n'
        m_writer.write(response.encode())
        await m_writer.drain()
        data = await m_reader.readline()
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
            data = await m_reader.readline()            
            length=int(data.decode().splitlines()[0].lstrip('$'))
            rdbfile=await m_reader.readexactly(length) 
            print(f"MASTER send RDB file of {length} bytes")
            commands_list=[]
        while True:
            try:
                print("Inside while loop, waiting on master command !!!!")
                ########Reading and parsing one command#########
                query_string,data_list=await get_command(m_reader)                           
                if not query_string or not data_list:
                    await asyncio.sleep(0.2)
                    continue
                # commands_list.append(data_list)

                command=query_string.encode()
                command_len=0
                print("COMMAND=",command) 
                if not command:
                    await asyncio.sleep(0.1)
                    continue               
                
                if command == b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n' or command == b'*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n' :
                    print('gectack found !!!!!command_offset is ',command_offset)
                    response=f'*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(command_offset))}\r\n{str(command_offset)}\r\n'  
                    m_writer.write(response.encode())  
                    await m_writer.drain() 
                    print('offset send to master for getack')
                    command_len=len(command)
                    command_offset=command_offset + command_len
                    RedisAsyncServer.replica_command_offset = command_offset
                    print("slave adding getack offset",command_offset)
                    command_len=0
                    continue     
                 
                print("processing command:",data_list)               
                if data_list[0] == 'SET':
                    command_len=0                        
                    RedisAsyncServer.data_store=set_commands.set_key(data_list,RedisAsyncServer.data_store)
                    command_string=''
                    # for s in command:
                    #     command_string=command_string+f'${len(str(s))}\r\n{str(s)}\r\n'
                    # command_string=f'*{len(command)}\r\n'+command_string
                    command_len=len(command)
                    # print("SET string",command_string)
                    # print("SET offset",command_len)
                    command_offset=command_offset+command_len
                    RedisAsyncServer.replica_command_offset = command_offset
                    print("slave adding SET offset",command_offset)
                    command_len=0
                    print(f'slave set the new value !!!!',RedisAsyncServer.data_store[key].data)
                elif data_list[0].upper() == 'REPLCONF' and data_list[1].upper() == 'GETACK': 
                    response=f'*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(RedisAsyncServer.replica_command_offset))}\r\n{str(RedisAsyncServer.replica_command_offset)}\r\n'  
                    m_writer.write(response.encode())  
                    await m_writer.drain() 
                    command_len=len(b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n')
                    command_offset=command_offset + command_len
                    RedisAsyncServer.replica_command_offset = command_offset
                    print("slave adding offset getack",command_offset)
                    command_len=0
                    continue     
                elif data_list[0].upper() == 'PING' :                        
                    command_len=14
                    command_offset=command_offset + command_len
                    RedisAsyncServer.replica_command_offset = command_offset
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
        global RedisAsyncServer
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
    global RedisAsyncServer
    master_details=''
    port_number=6379
    args=sys.argv
    RedisAsyncServer,port_number=setup_server(args,6379)    
    print("Execution starts here....!role=",RedisAsyncServer.role, master_details)    
    asyncio.run(run_server(port_number))    


if __name__ == "__main__":
    main()
