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

                
    

