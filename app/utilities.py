def is_member(userobjs,user):
    users_list=[obj.username for obj in userobjs]
    if user in users_list:
        return True
    return False

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


    

