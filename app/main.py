import socket  # noqa: F401
import asyncio

async def client_handler(reader,writer):
    try:
        
        CONNECT = True
        while CONNECT:
            input_data=await reader.read(1024)
            input_tokens=input_data.decode().splitlines()
            no_of_elements=int(input_tokens[0].lstrip('*'))
            i=1
            data_list=[]
            while i<no_of_elements:
                data_list.append(input_tokens[i+1].strip())
                i += 2
            if no_of_elements == 1:
                if data_list[0] == 'PING':
                    response=b"+PONG\r\n"
                    writer.write(response)
                    await writer.drain()                    
            elif no_of_elements > 1:
                if data_list[0] == 'ECHO':
                    if len(data_list[1:] ) > 1:
                        echo_data=" ".join(data_list[1:])
                    else:
                        echo_data = data_list[1]
                    string_length=len(echo_data)
                    response=f"${string_length}\r\n{echo_data}\r\n"  
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
