import socket  # noqa: F401
import asyncio
from datetime import timezone,timedelta,datetime

class RedisObject():
    def __init__(self,data,exp=None,counter=0):
        self.data = data
        self.exp = exp
        self.counter = counter

async def client_handler(reader,writer):
    try:
        data_store={}
        CONNECT = True
        while CONNECT:
            input_query=await reader.read(1024)
            if not input_query:
                break
            query_string=str(input_query.decode())            
            if not query_string.startswith("*"):
                break
            input_tokens=query_string.splitlines()
            no_of_elements=int(input_tokens[0].lstrip('*'))               

            data_list=[]
            if no_of_elements == 1:
                if input_tokens[2] == 'PING':
                    response=b"+PONG\r\n"
                    writer.write(response)
                    await writer.drain()                    
            elif no_of_elements > 1:
                for token in input_tokens:
                    if token.startswith('*') or token.startswith('$'):
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
                    expiry =None
                    if len(data_list) > 3:
                        if data_list[3] == 'PX':
                            expiry = datetime.now(timezone.utc) + timedelta(milliseconds=int(data_list[4]))
                        elif data_list[3] == 'EX' :
                            expiry = datetime.now(timezone.utc) + timedelta(seconds=int(data_list[4]))
                        
                    data_store[key] = RedisObject(data = val,exp=expiry) 
                    response=f"+OK\r\n"  
                    writer.write(response.encode())
                    await writer.drain() 
                elif data_list[0] == 'GET': 
                    key=data_list[1]
                    if key in data_store.keys() :
                        expiry=data_store[key].exp
                        if expiry < datetime.now(timezone.utc) :
                            response = f"$-1\r\n"
                        else:
                            val=data_store[key].data
                            val_length=len(val)
                            response=f'${val_length}\r\n{val}\r\n'                        

                    else:
                        response=f"$-1\r\n"
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
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    
    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # conn, _ =server_socket.accept() # wait for client        
    # while True :
    #     data = conn.recv(1024)
    #     conn.sendall(b"+PONG\r\n")

    asyncio.run(run_server())
    


if __name__ == "__main__":
    main()
