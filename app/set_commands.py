from datetime import datetime,timedelta,timezone
from .data_models import RedisObject,StreamEntry
from . utilities import valid_stream_key,get_new_stream_key
import time

# subscribe to channel
def subscribe(data_list,writer,client_address,channel_subscriptions):
    print('inside subscribe, datalist = ',data_list)
    if client_address not in channel_subscriptions:
        channel_subscriptions[client_address]=[]
        channel_subscriptions[client_address].append(set())
        channel_subscriptions[client_address].append(writer)
    print('inside subscribe, channel name = ',data_list[1])    
    channel_subscriptions[client_address][0].add(data_list[1])        
    return channel_subscriptions

#unsubscribe from channel
def unsubscribe(data_list,client_addr,channel_subscriptions):
    if client_addr in channel_subscriptions:
        if data_list[1] in channel_subscriptions[client_addr][0]:
            channel_subscriptions[client_addr][0].remove(data_list[1])  
    return channel_subscriptions   

#publish messages to channel
async def publish_message(data_list,channel_subscriptions):
    subscribers=0        
    if channel_subscriptions:
        for channels,subc_writer in channel_subscriptions.values():
            if data_list[1] in channels:
                subc_resp=f'*3\r\n$7\r\nmessage\r\n${len(data_list[1])}\r\n{data_list[1]}\r\n${len(data_list[2])}\r\n{data_list[2]}\r\n'
                subc_writer.write(subc_resp.encode())
                await subc_writer.drain() 
                subscribers += 1
    return subscribers

def get_data_type(val):
    if isinstance(val,str):
        return 'string'
    else:
        None

# set value for key
def set_key(data_list,data_store):
    key=data_list[1]
    val=data_list[2]
    data_type = get_data_type(val)
    expiry =None
    if len(data_list) > 3:
        if data_list[3] == 'PX':
            expiry = datetime.now(timezone.utc) + timedelta(milliseconds=int(data_list[4]))
        elif data_list[3] == 'EX' :
            expiry = datetime.now(timezone.utc) + timedelta(seconds=int(data_list[4]))
        
    data_store[key] = RedisObject(data = val,exp=expiry,data_type=data_type)
    return data_store

#############STREAMS########################
#XADD
def get_milliseconds_time():
    return int(time.time() * 1000)  

async def xadd(data_list,data_store):
    key=data_list[1]
    stream_key = data_list[2]
    # XADD key 0-1 foo bar
    #<millisecondsTime>-<sequenceNumber>
    AddStream = True
    if stream_key == '*':
        stream_key=str(get_milliseconds_time()) +'-*'                            
    stream_key_parts = str(stream_key).split('-')
    if key in data_store.keys() :
        redis_obj=data_store.get(key)   
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
            data_store[key] = RedisObject(data = [],data_type='stream') 
            redis_obj=data_store.get(key)
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
    return data_store,response,AddStream
        