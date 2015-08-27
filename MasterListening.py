import socket, datetime, time, threading,os
from pymongo import MongoClient
from subprocess import check_output
import sys


HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port
KEEPALIVE_TIME_GAP = 5; #seconds
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
STATE_DB_PORT = int(databaseCollection.find_one({'name':'State_DB'})['port'])
STATE_DB_CONN = MongoClient(STATE_DB, STATE_DB_PORT) 

def sleeper(seconds):
    time.sleep(seconds)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def openLogFile():
    now = datetime.datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    hour = now.strftime('%H')
    filePath = '/home/logsearch/harbinger/masterlistener/log/'
    filePath += year + month +'/'
    fileName = filePath + year + month + day + hour+'00.log'
    try:
            logFile = open(fileName, 'a')
            return logFile
    except IOError:
            mkdir_p(filePath)
            logFile = open(fileName, 'a')
            return logFile
#--------- End of indexScript.py

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
def checkMaster():
    output = check_output("ps x |grep -v grep |grep -c Master",shell=True)
    if int(output)< 2:
        masterLogFile = openLogFile()
        masterLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Error: Master is dead"+"\n") 
        masterLogFile.close()
        setIndexerToDead()        
        check_output(["pkill", "-9", "-f", "Master"])
        sys.exit()
def setIndexerToDead():
    db = MASTER_DB_CONN.logsearch
    indexerStateCollection = db.indexer_state
    indexerStateCollection.update_many({}, {"$set": {'state': 'dead'}}) 
    
class clientThread (threading.Thread):
    def __init__(self, conn, addr):
        threading.Thread.__init__(self)
        self.conn = conn
        self.addr = addr
    def run(self):
        self.conn.send('Welcome to the server. Type something and hit enter\n') #send only takes string
        jobID = ""
        masterListenerLogFile = openLogFile()
        #infinite loop so that function do not terminate and thread do not end.
        while True:
            # Receiving from client
            # Listening for Keep Alive Status
            try:
                data = self.conn.recv(1024)
                if(data==""):
                    break;
                keepAlive = extractCmd(data);
                jobID =  keepAlive[0]
                if keepAlive[1] =="indexing":
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Info: "+jobID+" got keep-alive:indexing from "+ self.addr[0]+":"+str(self.addr[1])
                    x = "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Info: "+jobID+" got keep-alive:indexing from "+ self.addr[0]+":"+str(self.addr[1])    
                    masterListenerLogFile.write(x+"\n")
                elif keepAlive[1] == "indexing-done":
                    # update indexer's state to wait_writing on MasterDB 
                    # changeStateMaster(keepAlive[0],'wait_writing')
                    changeStateState(keepAlive[0],'remove')
                    changeStateMaster(keepAlive[0],'remove')
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Info: "+jobID+" Indexing Done by: "+self.addr[0]+":"+str(self.addr[1])
                    masterListenerLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Info: "+jobID+" Indexing Done by: "+self.addr[0]+":"+str(self.addr[1])+"\n")
          
                    break
               
            except socket.timeout:
                    # update workingProcessDB setting state as dead 
                    changeStateMaster(jobID,'dead')
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Error: "+jobID+" connection timeout"
                    masterListenerLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Error: "+jobID+" connection timeout"+"\n")
          
                    break
            except socket.error:
                    # update workingProcessDB setting state as dead 
                    changeStateMaster(jobID,'dead')
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Error: connection closed: "+self.addr[0]+":"+str(self.addr[1])
                    masterListenerLogFile.write("["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Error: connection closed: "+self.addr[0]+":"+str(self.addr[1])+"\n")
                    break;
            except:
                    raise
                    break;
            #Timeout occurred, do things
        #if data == "done"        
        #came out of loop
        masterListenerLogFile.close()
        self.conn.close()
 

class CheckStateThread (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        #self.executeTime = executeTime
        #self.nextkeepAliveTime = nextkeepAliveTime
    def run(self):
        while True:
            #self.keepAliveTime = getExecuteTime()
            #if self.keepAliveTime >= self.nextkeepAliveTime:
            #    self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
            sleeper(KEEPALIVE_TIME_GAP)
            checkMaster()

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

# CheckMaster Thread
checkStateThread = CheckStateThread()
# Start new Threads
checkStateThread.start()

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    conn.settimeout(TIMEOUT)
    keepAliveThread = clientThread(conn,addr)
    # Start new Threads
    keepAliveThread.start()
    #print 'Connected with ' + addr[0] + ':' + str(addr[1])
 
s.close()