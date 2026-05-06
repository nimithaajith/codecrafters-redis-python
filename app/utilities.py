def is_member(userobjs,user):
    users_list=[obj.username for obj in userobjs]
    if user in users_list:
        return True
    return False


def client_exists(userobjs,client_address):
    addr_list=[obj.client_address for obj in userobjs]
    if client_address in addr_list:
        return True
    return False

def allow_commands(userobjs,client_address):
    addr_list=[obj.client_address for obj in userobjs]
    if client_address not in addr_list:        
        for user in userobjs:
            if user.username == 'default'  and ('nopass' not in user.flags):
                return False
    return True        

                
    

