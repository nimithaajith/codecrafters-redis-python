from . import geo_decode
from . import distance
from datetime import timezone,timedelta,datetime
from . utilities import get_xread_response,datastore_cleanup,key_expired
from . hashing import get_password

##############GEO SPATIAL#################################
# GEOSEARCH key FROMLONLAT longitude latitude BYRADIUS radius unit(m) 
def geo_search(data_list,data_store):
    try:
        key=data_list[1]
        locations=[]
        if key in data_store  and not key_expired(key,data_store):  
            geopos=data_store[key].data 
            center_long = float(data_list[3])
            center_lat= float(data_list[4])
            radius=float(data_list[6])
            unit=data_list[7]
            conv=1
            if unit == 'km':
                conv=0.001
            elif unit == 'mi' :
                conv =0.00062137

            for score,place in geopos:
                lat,long=geo_decode.decode(int(score))
                dist=  distance.haversine( lat, long,center_lat, center_long)
                if (dist * conv) <= radius :
                    locations.append(place)
            if locations :
                response=f'*{len(locations)}\r\n'+''.join(f'${len(l.encode())}\r\n'.encode()+l.encode()+b'\r\n' for l in locations)
            else:
                response=b'*0\r\n'
        else:
            response=b'*0\r\n'
    except RuntimeError as e:
        response=f"-ERR : GEO SEARCH failed ,{str(e)}.\r\n".encode() 
    except Exception as e:
        response=f"-ERR : GEO SEARCH failed , {str(e)}\r\n".encode() 
    return response

# GEODIST places Munich Paris  
def geo_dist(data_list,data_store):
    try:
        key=data_list[1]
        locations=data_list[2:]            
        if key in data_store and not key_expired(key,data_store):                
            geopos=data_store[key].data 
            latitude1 = None
            longitude1= None
            latitude2= None
            longitude2 = None
            for score,location in geopos:
                if location in locations:
                    if locations[0] == locations[1]:
                        response=b'$1\r\n0\r\n'
                        break
                    if location == locations[0]:
                        latitude1,longitude1=geo_decode.decode(int(score))
                    elif location == locations[1]:
                        latitude2,longitude2=geo_decode.decode(int(score))            
                if  latitude1 and longitude1 and latitude2 and longitude2 :
                    dist=  distance.haversine(latitude1, longitude1, latitude2, longitude2) 
                    dist_bytes=str(dist).encode() 
                    response=f'${len(dist_bytes)}\r\n'.encode() + dist_bytes + b'\r\n'
                    break  
        else:
            response = b'$-1\r\n'
    except RuntimeError as e:
        response=f"-ERR : GEO DIST failed ,{str(e)}.\r\n".encode()  
    except Exception as e:
        response=f"-ERR : GEO DIST failed , {str(e)}\r\n".encode() 
    return response
    
# GEOPOS key member1 member2
def geo_position(data_list,data_store):
    try:
        key=data_list[1]
        members=data_list[2:]
        m_len=len(members)
        response=f'*{m_len}\r\n'.encode()
        if key in data_store and not key_expired(key,data_store):                
            geopos=data_store[key].data                
            for query_member in members:
                is_member=False
                for score,member in geopos:
                    if member == query_member:
                        latitude,longitude=geo_decode.decode(int(score))
                        la=str(latitude)
                        lo=str(longitude)
                        la_bytes=la.encode()
                        lo_bytes=lo.encode()
                        response=response + f'*2\r\n${len(lo_bytes)}\r\n'.encode() + lo_bytes + f'\r\n${len(la_bytes)}\r\n'.encode() + la_bytes + b'\r\n'
                        is_member=True
                        break
                if not is_member:
                    response=response + b'*-1\r\n'
        else:
            response=response + ''.join(b'*-1\r\n' for _ in range(m_len))
    except RuntimeError as e:
        response=f"-ERR : GEO POSITION failed ,{str(e)}.\r\n".encode()  
    except Exception as e:
        response=f"-ERR : GEO POSITION failed , {str(e)}\r\n".encode() 
    
    return response


########SORTED SET###########################
# ZSCORE zset_key member
def get_zscore(data_list,data_store):
    try:
        key = data_list[1]
        member=data_list[2]
        if key not in data_store or key_expired(key,data_store) :
            response = b"$-1\r\n" 
        else:
            is_member = False
            old_data = data_store[key].data
            for l in old_data:
                if l[1] == member : 
                    scr_str=str(l[0]) 
                    scr_bytes=  scr_str.encode()                                                                 
                    response=f'${len(scr_bytes)}\r\n'.encode()+scr_bytes+b'\r\n' 
                    is_member = True                      
                    break
            if not is_member:
                response = b'$-1\r\n'
    except RuntimeError as e:
        response=f"-ERR : Getting zscore failed ,{str(e)}.\r\n".encode()  
    except Exception as e:
        response=f"-ERR : Getting zscore  failed , {str(e)}\r\n".encode() 
    
    return response  

# ZCARD zset_key
def get_zcard(data_list,data_store) :
    try:
        key = data_list[1]            
        if key not in data_store or key_expired(key,data_store) :
            response = ":0\r\n"         
        else:
            scores=data_store[key].data
            total_len=len(scores)
            response = f":{total_len}\r\n"  
    except RuntimeError as e:
        response=f"-ERR : Getting zcard failed ,{str(e)}.\r\n"  
    except Exception as e:
        response=f"-ERR : Getting zcard  failed , {str(e)}\r\n" 
    
    return response  

# ZRANGE racer_scores 0 2
def get_zrange(data_list,data_store) :
    try:
        key=data_list[1]
        start_inx=int(data_list[2])
        end_inx=int(data_list[3])
        slice = True
        if key not in data_store or key_expired(key,data_store):
            response = '*0\r\n' 
            slice = False
        else:
            scores=data_store[key].data
            total_len=len(scores)
            # If the start index is greater than or equal to the cardinality of the sorted set, an empty array is returned.
            #If the start index is greater than the stop index, the result is an empty array
            if start_inx >= total_len :
                response = '*0\r\n' 
                slice = False
            if  end_inx >= 0 and start_inx > end_inx :
                response = '*0\r\n' 
                slice = False
            if slice:
                # If the stop index is greater than the cardinality of the sorted set, the stop index is treated as the last element.
                if end_inx > total_len :
                    end_inx = total_len-1
                if end_inx < 0:
                    if abs(end_inx) >= total_len :
                        end_inx = 0
                    else:
                        end_inx = total_len + end_inx
                if start_inx < 0:
                    if abs(start_inx) >= total_len :
                        start_inx = 0
                    else: 
                        start_inx = start_inx +total_len
                
                sliced_set= scores[start_inx : end_inx+1]
                response=f'*{len(sliced_set)}\r\n'
                response=response+''.join(f'${len(l[1])}\r\n{l[1]}\r\n' for l in sliced_set) 
    except RuntimeError as e:
        response=f"-ERR : Getting zrange failed ,{str(e)}.\r\n"  
    except Exception as e:
        response=f"-ERR : Getting zrange  failed , {str(e)}\r\n"     
    return response


 # ZRANK zset_key member
def get_zrank(data_list,data_store):
    try:
        key=data_list[1]
        member = data_list[2] 
        if key not in data_store or key_expired(key,data_store):
            response = '$-1\r\n' 
        else:
            memberExists = False
            old_data = data_store[key].data
            for l in old_data:
                if l[1] == member :
                    index=old_data.index(l)                                             
                    response=f':{index}\r\n' 
                    memberExists = True                      
                    break
            if not memberExists:
                response = '$-1\r\n'
    except RuntimeError as e:
        response=f"-ERR : Getting zrank failed ,{str(e)}.\r\n"  
    except Exception as e:
        response=f"-ERR : Getting zrank  failed , {str(e)}\r\n" 
    
    return response

##############GENERAL#############################
# CONFIG GET
def config_get(data_list,RedisAsyncServer):
    response=''
    if data_list[2].lower() == 'dir':
        rdb_dir=RedisAsyncServer.rdb_dir
        if rdb_dir is not None:
            rdb_dir_bytes=rdb_dir.encode()
            response=f'*2\r\n$3\r\ndir'.encode()+f'\r\n${len(rdb_dir_bytes)}\r\n'.encode()+rdb_dir_bytes+b'\r\n'
        else:
            path=RedisAsyncServer.dir
            path_bytes=path.encode()
            response=f'*2\r\n$3\r\ndir\r\n${len(path_bytes)}\r\n'.encode()+path_bytes+b'\r\n'
    elif data_list[2].lower() == 'dbfilename':
        rdb_filename=RedisAsyncServer.rdb_filename
        response=f'*2\r\n$10\r\ndbfilename\r\n${len(rdb_filename.encode())}\r\n'.encode()+rdb_filename.encode()+b'\r\n'
    elif data_list[2].lower() == 'appendonly':
        aof_status=RedisAsyncServer.appendonly
        response=f'*2\r\n$10\r\nappendonly\r\n${len(aof_status.encode())}\r\n'.encode()+aof_status.encode()+b'\r\n'
    elif data_list[2].lower() == 'appenddirname':
        appenddir=RedisAsyncServer.appenddirname
        response=f'*2\r\n$13\r\nappenddirname\r\n${len(appenddir.encode())}\r\n'.encode()+appenddir.encode()+b'\r\n'
    elif data_list[2].lower() == 'appendfilename':
        appendfile=RedisAsyncServer.appendfilename
        response=f'*2\r\n$14\r\nappendfilename\r\n${len(appendfile.encode())}\r\n'.encode()+appendfile.encode()+b'\r\n'
    elif data_list[2].lower() == 'appendfsync':
        append_sync=RedisAsyncServer.appendfsync
        response=f'*2\r\n$11\r\nappendfsync\r\n${len(append_sync.encode())}\r\n'.encode()+append_sync.encode()+b'\r\n'
    return response

# KEYS
def get_keys(data_list,data_store):
    try:
        data_store=datastore_cleanup(data_store)
        all_keys=data_store.keys()
        if data_list[1] == '*':        
            response=f'*{len(all_keys)}\r\n'.encode()    
            for k in all_keys:
                k_bytes=k.encode()
                response=response+f'${len(k_bytes)}\r\n'.encode()+k_bytes+b'\r\n'
        else:
            pattern=data_list[1]
            k_count=0
            response=''
            import fnmatch
            for k in all_keys:
                if fnmatch.fnmatch(k, pattern):
                    response=response+f'${len(k.encode())}\r\n'.encode()+k.encode()+b'\r\n'
                    k_count+=1
            response=f'*{k_count}\r\n'.encode()+response
    except Exception as e:
        response=f"-ERR : Getting key(s)  failed , {str(e)}\r\n".encode() 
    
    return response

#GETUSER
def get_user(data_list,current_users):
    user_name=data_list[2]
    user=current_users[0]
    flags=user.flags
    if 'nopass' in flags:
        response = '*4\r\n$5\r\nflags\r\n*1\r\n$6\r\nnopass\r\n$9\r\npasswords\r\n*0\r\n'
    else:
        try:
            # password=get_password(user.password)
            password = user.password
            response = f'*4\r\n$5\r\nflags\r\n*0\r\n$9\r\npasswords\r\n*1\r\n${len(password)}\r\n{password}\r\n'
        except Exception as e:
            response="-Err password retrieval failed"
    return response


#TYPE
def get_type(data_list,data_store) :  
    try:      
        key=data_list[1]
        if key in data_store.keys() and not key_expired(key,data_store):
            data_type= data_store.get(key).data_type
            response=f'+{data_type}\r\n'
        else:
            response=f'+none\r\n'
    except RuntimeError as e:
        response=f"-ERR : Getting type failed ,{str(e)}.\r\n"  
    except Exception as e:
        response=f"-ERR : Getting type  failed , {str(e)}\r\n" 
    
    return response

################ STREAMS ################################
# xread
def xread_streams(data_list,data_store):
    try:
        xread_list=data_list[2:]
        #keys of streams to be read from data_store
        no_of_keys=int(len(xread_list)//2)
        xread_dict={}
        for i in range(no_of_keys):
            xread_dict[xread_list[i]] =xread_list[i+no_of_keys]
        if xread_dict:
            response=f'*{no_of_keys}\r\n'
        for key,stream_key in xread_dict.items():        
            key_response=''
            redis_obj=None
            if key in data_store.keys() and not key_expired(key,data_store):                                
                redis_obj = data_store[key]
                key_response,_= get_xread_response(key,redis_obj,stream_key)
                response = response + key_response 
    except RuntimeError as e:
        response=f"-ERR : Stream reading failed ,{str(e)}.\r\n"  
    except Exception as e:
        response=f"-ERR : stream reading  failed , {str(e)}\r\n"             
    return response


