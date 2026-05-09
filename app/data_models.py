from collections import deque

class Transaction():
    locks={}

class User():
    def __init__(self):
        self.username='default'
        self.flags=['nopass']
        self.password=''
        self.client_address=[]

class RedisServer():
    def __init__(self,role,port=6379):
        self.port= port
        self.host='localhost'
        self.role = role        
        self.master_host=None
        self.master_port=None
        self.data_store={}
        # self.server=Master()
        self.clients=[]

class Replica(RedisServer):
    def __init__(self):
        super().__init__(role='slave' )
        self.replica_command_offset=0
        print('replica offset initialized to 0')
        
class Master(RedisServer):
    def __init__(self):
        super().__init__(role='master')
        self.rdb_filename=None
        self.rdb_dir=None
        self.dir='/app'
        self.appendonly='no'
        self.appenddirname='appendonlydir'
        self.appendfilename='appendonly.aof'
        self.appendfsync='everysec'
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
