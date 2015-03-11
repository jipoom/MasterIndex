import socket, datetime, time, threading
from pymongo import MongoClient
import sys


HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port
KEEPALIVE_TIME_GAP = 2; #seconds
TIMEOUT = 5
#MASTER_DB = "192.168.1.42"
#STATE_DB = "192.168.1.42"
#MASTER_DB_PORT = 27017
#STATE_DB_PORT = 27017
#MasterDBConn = MongoClient(MASTER_DB, MASTER_DB_PORT)
#StateDBConn = MongoClient(STATE_DB, STATE_DB_PORT) 
def retrieveCollection(conn,dbName,colName):
    # Get last record from state DB
    db = conn[dbName]
    collection = db[colName]
    #conn.close()
    return collection
MASTER_DB = sys.argv[1]
MASTER_DB_PORT = int(sys.argv[2])
MASTER_DB_CONN = MongoClient(MASTER_DB, MASTER_DB_PORT)
databaseCollection = retrieveCollection(MASTER_DB_CONN,"logsearch","Database_config")
STATE_DB = databaseCollection.find_one({'name':'State_DB'})['ip_addr']
STATE_DB_PORT = databaseCollection.find_one({'name':'State_DB'})['port']
STATE_DB_CONN = MongoClient(STATE_DB, STATE_DB_PORT) 

def changeStateMaster(jobID, state):
    db = MASTER_DB_CONN.logsearch
    indexerStateCollection = db.indexer_state
    if state == 'remove':
        indexerStateCollection.remove({'jobID': jobID})
    else:    
        indexerStateCollection.update({'jobID': jobID}, {"$set": {'state': state}})

    
def changeStateState(jobID, state):
    db = STATE_DB_CONN.logsearch
    indexerStateCollection = db.StateDB_state
    if state == 'remove':
        indexerStateCollection.remove({'jobID': jobID})
        
# Extract CMD    
def extractCmd(cmd):
    return cmd.split('##')   

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  


class clientThread (threading.Thread):
    def __init__(self, conn, addr):
        threading.Thread.__init__(self)
        self.conn = conn
        self.addr = addr
    def run(self):
        self.conn.send('Welcome to the server. Type something and hit enter\n') #send only takes string
        jobID = ""
        #infinite loop so that function do not terminate and thread do not end.
        while True:
            # Receiving from client
            # Listening for Keep Alive Status
            try:        
                data = self.conn.recv(1024)
                keepAlive = extractCmd(data);
                jobID =  keepAlive[0]
                if keepAlive[1] =="indexing":
                    print "got keep-alive:indexing from "+ self.addr[0]+":"+str(self.addr[1])
                elif keepAlive[1] =="writing":
                    print "got keep-alive:writing from "+ self.addr[0]+":"+str(self.addr[1])
                elif keepAlive[1] == "indexing-done":
                    # update indexer's state to wait_writing on MasterDB 
                    # changeStateMaster(keepAlive[0],'wait_writing')
                    changeStateState(keepAlive[0],'remove')
                    changeStateMaster(keepAlive[0],'remove')
                    print "Indexing Done by: "+self.addr[0]+":"+str(self.addr[1])
                    break
                elif keepAlive[1] == "writing-done":
                    # Remove indexer state on MasterDB for this task 
                    # Remove indexer state on StateDB for this task 
                    changeStateState(keepAlive[0],'remove')
                    changeStateMaster(keepAlive[0],'remove')
                    print "Writing Done by: "+self.addr[0]+":"+str(self.addr[1])
                    break
                #if not data: 
                #    break
     
                #conn.sendall(reply)
            except socket.timeout:
                    # update workingProcessDB setting state as dead 
                    changeStateMaster(jobID,'dead')
                    print "timeout"
                    break
            except socket.error:
                    # update workingProcessDB setting state as dead 
                    changeStateMaster(jobID,'dead')
                    print "close: "+self.addr[0]+":"+str(self.addr[1])
                    break;
            #Timeout occurred, do things
        #if data == "done"        
        #came out of loop
        self.conn.close()
 


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'
 
#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
     
print 'Socket bind complete'
 
#Start listening on socket
s.listen(5)
print 'Socket now listening'

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    conn.settimeout(TIMEOUT)
    keepAliveThread = clientThread(conn,addr)
    # Start new Threads
    keepAliveThread.start()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
 
s.close()