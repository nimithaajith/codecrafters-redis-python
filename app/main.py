import asyncio
from datetime import timezone,timedelta,datetime
import time

def get_milliseconds_time():
    return int(time.time() * 1000)

class RedisObject():
    def __init__(self,data=None,data_type=None,exp=None,counter=0):
        self.data = data
        self.exp = exp
        self.counter = counter
        self.data_type = data_type
        self.last_key=None

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
    for stream_obj in redis_obj.data:
        l=0
        if stream_obj.id >= start and stream_obj.id <= end:
            l=len(stream_obj.entry)
            result.append((l,stream_obj))
    n1 = len(result)
    print("result length =",n1)
    result_str=f'*{n1}\r\n'
    for length,obj in result:
        print(">>>length,obj =",length,obj)
        result_str=result_str+f'*{length}\r\n${len(obj.id)}\r\n{obj.id}\r\n'
        for k,v in obj.entry.items():
            print(">>>>>>k,v =",k,v)
            result_str=result_str+f'${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n'
    return result_str   
    
        

async def get_data_type(val):
    if isinstance(val,str):
        return 'string'
    else:
        None

async def client_handler(reader,writer):
    try:
        print("Connected...")
        data_store={}
        CONNECT = True
        while CONNECT:
            input_query=await reader.read(1024)
            print("Received query :",input_query)
            if not input_query:
                break
            query_string=str(input_query.decode()) 
            print("query_string :",query_string)           
            if not query_string.startswith("*"):
                break
            input_tokens=query_string.splitlines()
            print("input_tokens :",input_tokens) 
            no_of_elements=int(input_tokens[0].lstrip('*'))               
            print("no_of_elements :",input_tokens[0], no_of_elements) 
            data_list=[]
            if no_of_elements == 1:
                if input_tokens[2] == 'PING':
                    response=b"+PONG\r\n"
                    writer.write(response)
                    await writer.drain()                    
            elif no_of_elements > 1:
                for token in input_tokens:
                    if len(token)>1 and token.startswith('*'):
                        continue
                    if token.startswith('$'):
                        continue
                    data_list.append(token.strip())
                if data_list[0] == 'ECHO':
                    if len(data_list[1:] ) > 1:
                        echo_data=" ".join(data_list[1:])
                    else:
                        echo_data = data_list[1]
                    string_length=len(echo_data)
                    response=f"${string_length}\r\n{echo_data}\r\n"  
                    writer.write(response.encode())
                    await writer.drain() 
                elif data_list[0] == 'SET':
                    key=data_list[1]
                    val=data_list[2]
                    data_type = await get_data_type(val)
                    expiry =None
                    if len(data_list) > 3:
                        if data_list[3] == 'PX':
                            expiry = datetime.now(timezone.utc) + timedelta(milliseconds=int(data_list[4]))
                        elif data_list[3] == 'EX' :
                            expiry = datetime.now(timezone.utc) + timedelta(seconds=int(data_list[4]))
                        
                    data_store[key] = RedisObject(data = val,exp=expiry,data_type=data_type) 
                    response=f"+OK\r\n"  
                    writer.write(response.encode())
                    await writer.drain() 
                elif data_list[0] == 'GET': 
                    key=data_list[1]
                    if key in data_store.keys() :
                        val=data_store[key].data
                        val_length=len(val)
                        response=f'${val_length}\r\n{val}\r\n'
                        expiry=data_store[key].exp
                        if expiry :
                            if expiry < datetime.now(timezone.utc) :
                                response = f"$-1\r\n"                       
                            
                    else:
                        response=f"$-1\r\n"
                    writer.write(response.encode())
                    await writer.drain() 
                elif data_list[0] == 'XADD': 
                    key=data_list[1]
                    stream_key = data_list[2]
                    print('key =',key,' stream key =',stream_key)                    
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
                        print('l =',l)
                        print('new_stream_entry =',new_stream_entry.id, new_stream_entry.entry)
                        new_stream_entry.add_entry(l)
                        print('new_stream_entry =',new_stream_entry.id, new_stream_entry.entry)
                    
                    
                        redis_obj.data.append(new_stream_entry)
                        print('redis obj data')
                        print(redis_obj.data)
                        response=f'${len(stream_key)}\r\n{stream_key}\r\n'  
                    else:
                        response = message
                    writer.write(response.encode())
                    await writer.drain() 
                elif data_list[0] == 'XRANGE': 
                    key=data_list[1] 
                    response=''
                    if key in data_store.keys():
                        print(">>>>>DATA LIST <<<<<<<<") 
                        print(data_list)
                        start=data_list[2]
                        stop=data_list[3]
                        redis_obj = data_store[key]
                        response=get_xrange_response(redis_obj,start,stop)
                        print("start-end:::",start,stop)
                        print(response)
                    writer.write(response.encode()) 
                    await writer.drain()     
                elif data_list[0] == 'TYPE': 
                    key=data_list[1]
                    if key in data_store.keys() :
                        data_type= data_store.get(key).data_type
                        response=f'+{data_type}\r\n'
                    else:
                        response=f'+none\r\n'
                    writer.write(response.encode())
                    await writer.drain()                                           
            
            if not CONNECT:
                break
                
    except Exception as e:
        print("Client handling failed : Error ->",str(e))
    writer.close()
    await writer.wait_closed()



async def run_server():
    try:
        redis_server=await asyncio.start_server(client_handler,host="localhost",port=6379)
        print(f'Redis server listening {redis_server.sockets[0].getsockname()}')
        await redis_server.serve_forever()
    except Exception as e:
        print("Server execution failed : Error ->",str(e))

def main():
    print("Execution starts here....!")

    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # conn, _ =server_socket.accept() # wait for client        
    # while True :
    #     data = conn.recv(1024)
    #     conn.sendall(b"+PONG\r\n")

    asyncio.run(run_server())
    


if __name__ == "__main__":
    main()
