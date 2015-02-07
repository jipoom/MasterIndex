import datetime, time, threading, socket, string, random, pymongo
from pymongo import MongoClient
# lists containing alive process
# eg. 192.168.1.1:12345
# CONSTANT
EXECUTE_TIME_GAP = 5; #seconds
KEEPALIVE_TIME_GAP = 2; #seconds
MASTER_DB = "192.168.1.42"
STATE_DB = "192.168.1.42"
MASTER_DB_PORT = 27017
STATE_DB_PORT = 27017
processList = [("127.0.0.1","9999")]
aliveList = []
rankedList = []
uniquePath = []
now=datetime.datetime.now()
nextExecuteTime = time.mktime(now.timetuple())+5
nextkeepAliveTime = time.mktime(now.timetuple())+KEEPALIVE_TIME_GAP
MasterDBConn = MongoClient(MASTER_DB, MASTER_DB_PORT)
StateDBConn = MongoClient(STATE_DB, STATE_DB_PORT) 

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  

def changeState(cmd, jobID, state, node, dbNode, order):
    # is called in case an error is found
    # insert state into MasterDB
    db = MasterDBConn.logsearch
    indexerStateCollection = db.indexer_state
    if cmd == "insert": 
        if state == "wait_writing":
            document = {
                        'jobID':jobID,
                        'state':state,
                        'node':node,
                        'db':dbNode,
                        'lastDoneRecord':order
                        }
        else:    
            order = extractCmd(order)
            if order[5] == 'multiLine':
                document = {
                            'jobID':jobID,
                            'state':state,
                            'indexer':node,
                            'db':dbNode,
                            'service':order[0],
                            'system':order[1],
                            'node':order[2],
                            'process':order[3],
                            'path':order[4],
                            'logType':order[5],
                            'logStartTag':order[6],
                            'logEndTag':order[7],
                            'msisdnRegex':order[8],
                            'dateHolder':order[9],
                            'dateRegex':order[10],
                            'dateFormat':order[11],
                            'timeRegex':order[12],
                            'timeFormat':order[13],
                            'mmin':order[14],
                            'interval':order[15],
                            'lastDoneRecord':order[16]    
                            }
            elif order[5] == 'singleLine':
                document = {
                            'jobID':jobID,
                            'state':state,
                            'indexer':node,
                            'db':dbNode,
                            'service':order[0],
                            'system':order[1],
                            'node':order[2],
                            'process':order[3],
                            'path':order[4],
                            'logType':order[5],
                            'msisdnRegex':order[6],
                            'dateHolder':order[7],
                            'dateRegex':order[8],
                            'dateFormat':order[9],
                            'timeRegex':order[10],
                            'timeFormat':order[11],
                            'mmin':order[12],
                            'interval':order[13],
                            'lastDoneRecord':order[14]      
                            }
        indexerStateCollection.insert(document)
    elif cmd == "update":
        if order == "":
            indexerStateCollection.update({'jobID': jobID}, {"$set": {'state': state, 'indexer': node}})
        else:
            indexerStateCollection.update({'jobID': jobID}, {"$set": {'state': state, 'lastDoneRecord':order}})
    print "changeState"

def generateJobID(size=10, chars=string.ascii_uppercase + string.digits+string.ascii_lowercase):
    # generates jobID for tasks
    # return jobID
    print "jobID"
    return ''.join(random.choice(chars) for _ in range(size))

def isOldJob(jobID):
    # check on MasterDB if this job exists
    # return boolean
    print "is old job?"
     
def getTask(mode):
    #mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    #db = mongoClient.logsearch
    if mode == "routine":
        # Get tasks configured by users from MastDB
        taskCollection = retrieveCollection(MasterDBConn,'logsearch','service_config')
        #taskCollection = db.service_config
    elif mode == "error":
        taskCollection = retrieveCollection(MasterDBConn,'logsearch','indexer_state')
        #taskCollection = db.indexer_state
    #mongoClient.close();
    # return all tasks in List
    return taskCollection.find() # dictionary type
    print "getTask"

def getRecordFromStateDB():
    # Get last record from state DB
    mongoClient = MongoClient(STATE_DB, STATE_DB_PORT)
    db = mongoClient.logsearch
    stateCollection = db.StateDB_state
    mongoClient.close()
    # return string containing jobID:state:last_record:node
    print "getRecordStateDB"
    return stateCollection


def retrieveCollection(conn,dbName,colName):
    # Get last record from state DB
    db = conn[dbName]
    collection = db[colName]
    #conn.close()
    return collection
  
# keepAlive is to test if each process is still alive
def checkIndexerState():
    #mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    #db = mongoClient.logsearch
    #IndexerStateCollection = db.indexer_state
    IndexerStateCollection = retrieveCollection(MasterDBConn,'logsearch','indexer_state')
    deadIndexer = IndexerStateCollection.find({'state':'dead'})
    if deadIndexer.count() > 0:
        # Create new thread (ErrorRecoveryThread)
        errRecv = ErrorRecoveryThread()
        errRecv.start()
        print 'dead exists'
    queingIndexer = IndexerStateCollection.find({'state':'wait_indexing'})
    queingWriter = IndexerStateCollection.find({'state':'wait_writing'})
    if queingIndexer.count() + queingWriter.count() > 0:
        # Create new threads (TriggerProcess error mode)
        triggerProcess('error')
        print 'queingIndexer or queingWriter exists'
    #mongoClient.close();            
    print "checkIndexerState"
    
# Extract CMD    
def extractCmd(cmd):
    return cmd.split('##')
    
 
# TriggerProcess is to trigger process to work
def triggerProcess(mode):
    tasks = getTask(mode)
    triggerProcess = TriggerThread(tasks)
    triggerProcess.start()
    #triggerProcess.join()
    print "TriggerProcess: "+str(nextExecuteTime)    
# rankProcess is to rank all processes by performance
def rankProcess(indexerList): 
    print "rankProcess"
    performance = []
    for indexer in indexerList:
        # SNMP to test CPU and memory
        performance.append('result'+indexer['name'])
    # reorder ranked process
    for indexer in performance:
        print indexer
        # sort indexers according to performance result
    # return  
    return indexerList
# assignTask is to assign tasks to processes
def sendTask(indexerIpAddr,indexerPort,order): 
    server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
    #infinite loop so that function do not terminate and thread do not end.
    try:
        server.connect ( ( indexerIpAddr, indexerPort ) )
        server.send (order)
        server.close()       
    except socket.error:
        #came out of loop
        server.close()
    # send cmd to the specified indexer
    # update MasterDB setting state as "indexing"
    # return 
    print "sendTask"     
    
def checkDBPerformace(host):
    # check DB workload
    # return T of F
    #mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    #db = mongoClient.logsearch
    #print db.command("collstats", "indexer_state")
    #print db.command("currentOp")
    conn = pymongo.connection.Connection(host, 27017)
    all_ops = conn['admin']['$cmd.sys.inprog'].find_one('inprog')['inprog']
    active_ops = [op for op in all_ops if op['active']]
    conn.close();
    print '%d/%d active operations' % (len(active_ops), len(all_ops))
    print "checkDBPerformace"
    
def getIndexer(): 
    # get indexers from MasterDB
    print "getIndexer"  
    indexerCollection = retrieveCollection(MasterDBConn,'logsearch','MasterDB_indexer')
    indexerList = []
    for indexer in indexerCollection.find():
        indexerDict = {
                       'id': indexer['_id'],
                       'name':indexer['name'],
                       'ip_addr':indexer['ip_addr'],
                       'port':indexer['port']
                       }
        indexerList.append(indexerDict)
    return indexerList # dictionary type
     

class TriggerThread (threading.Thread):
    def __init__(self,tasks):
        self.process = None
        threading.Thread.__init__(self)
        self.tasks = tasks
    def run(self):
        # Get all indexer
        indexerList = getIndexer()
        # rank all processes
        rankedIndexer = rankProcess(indexerList)
        print indexerList
        order = ""
        # tasks = getTask()
        # Iterate over ranked list and uniquePath and call sendTask(indexer,cmd)
        for i in range(0, self.tasks.count()):  
            # wait_indexing found
            indexerIPAddr = rankedIndexer[i%len(rankedIndexer)]['ip_addr']
            indexerPort = rankedIndexer[i%len(rankedIndexer)]['port']
            if self.tasks[i]['state'] == 'wait_indexing':
                # build cmd for indexer to run still missing the starting point (line number)
                if self.tasks[i]['logType'] == 'singleLine':
                    cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+self.tasks[i]['mmin']+'##'+self.tasks[i]['interval']+'##'+self.tasks[i]['lastDoneRecord']
                elif self.tasks[i]['logType'] == 'multiLine':
                    cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['logStartTag']+"##"+self.tasks[i]['logEndTag']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+self.tasks[i]['mmin']+'##'+self.tasks[i]['interval']+'##'+self.tasks[i]['lastDoneRecord']
                jobId = self.tasks[i]['jobID']
                order = "indexing##"+jobId+"##"+cmd
                print "wait_indexing"
                # call changeState to update state on MasterDB
                changeState("update", jobId, "indexing", rankedIndexer[i%len(rankedIndexer)]['name'], "","")
            # wait_writing found
            elif self.tasks[i]['state'] == 'wait_writing':
                # if indexingDB is working less than 5000 records/sec
                if self.tasks[i]['db'] == '':
                    cmd = "withoutDB"
                else:
                    cmd = "withDB##"+self.tasks[i]['db'];
                jobId = self.tasks[i]['jobID']
                order = "writing##"+jobId+"##"+cmd
                print "wait_writing"
                print order
                # print order
                # print rankedIndexer[i%len(rankedIndexer)]['name']+"-"+jobId 
                # call changeState to add state on MasterDB
                changeState("update", jobId, "writing", rankedIndexer[i%len(rankedIndexer)]['name'], "","")
            # routing task
            elif self.tasks[i]['state'] == 'new':
                # build cmd for indexer to run still missing the starting point (line number)
                if self.tasks[i]['logType'] == 'singleLine':
                    cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+self.tasks[i]['mmin']+'##'+self.tasks[i]['interval']+'##'+self.tasks[i]['lastDoneRecord']
                elif self.tasks[i]['logType'] == 'multiLine':
                    cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['logStartTag']+"##"+self.tasks[i]['logEndTag']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+self.tasks[i]['mmin']+'##'+self.tasks[i]['interval']+'##'+self.tasks[i]['lastDoneRecord']
          
                # generate JobID
                jobId = generateJobID()
                # assign jobID to each node
                order = "indexing##"+jobId+"##"+cmd
                # print "indexing"
                # print order
                # print rankedIndexer[i%len(rankedIndexer)]['name']+"-"+jobId 
                # call changeState to add state on MasterDB
                changeState("insert", jobId, "indexing", rankedIndexer[i%len(rankedIndexer)]['name'], "",cmd)
            # send tasks to indexers
            # sendTask(indexerIPAddr,indexerPort,order)

class WritingThread (threading.Thread):
    def __init__(self,host,port):
        self.process = None
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
    def run(self):
        print "Writing"
        # Call checkDB to check IndexingDB performance
        # if indexingDB is working less than 5000 records/sec
            # if state = "wait_writing" and DB != ""
                # Assign new indexer to write the rest records from DB 
            # else
                # Call sendWritingSignal(indexer, write)
                # Change wait_writing to writing on MasterDB
       
        server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        server.connect ( ( self.host, self.port ) )
        #infinite loop so that function do not terminate and thread do not end.
        try:
            server.send ('writing')
            server.close()       
        except socket.error:
            #came out of loop
            server.close()

class ErrorRecoveryThread (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        print "ErrorRecoveryThread"
        # get DeadIndexer
        indexerStateCollection = retrieveCollection(MasterDBConn,'logsearch','indexer_state')
        deadIndexer = indexerStateCollection.find({'state':'dead'})
        stateDBCollection = retrieveCollection(StateDBConn,'logsearch','StateDB_state')
        indexerDetailCollection = retrieveCollection(MasterDBConn,'logsearch','MasterDB_indexer')

        for i in range(0, deadIndexer.count()):
            # get before-dead state
            oldIndexer = stateDBCollection.find_one({'jobID':deadIndexer[i]['jobID']})   
            ### check StateDB for the dead indexer then set ignore bit = 1 for this record
            # Get Dead Idx detail
            deadIdxDetail = indexerDetailCollection.find_one({'name':deadIndexer[i]['indexer']})  
            # if stateDB shows "indexing"
            if oldIndexer['state'] == "indexing":           
                # if local DB of the dead indexer is alive
                try:
                    checkLocalDB = MongoClient(deadIdxDetail['ip_addr'], MASTER_DB_PORT)
                    checkLocalDB.close()
                    # Creat new job ID for writing
                    jobId = generateJobID()
                    # for aldeary indexed records
                    changeState("insert", jobId, "wait_writing", "",deadIdxDetail['ip_addr'],"0")
                    # for non-indexed records
                    changeState("update", oldIndexer['jobID'], "wait_indexing", "", "",oldIndexer['lastDoneRecord'])
                # if local DB of the dead indexer is also dead
                except pymongo.errors.ConnectionFailure:
                    # start over from the beginning
                    changeState("update", oldIndexer['jobID'], "wait_indexing", "", "","0")               
                    
            # if stateDB shows "writing"
            if oldIndexer['state'] == "writing":
                # if local DB of the dead indexer is alive
                try:
                    testLocalDB = MongoClient(deadIdxDetail['ip_addr'], MASTER_DB_PORT)
                    testLocalDB.close()
                    changeState("update", oldIndexer['jobID'], "wait_writing", "", deadIdxDetail['ip_addr'],"0")
                    # get the last written record of the dead indexer from stateDB
                    # Call addTask(wait_writing, indexer's local_DB) to add Task to MasterDB 
                except pymongo.errors.ConnectionFailure:
                    # start over from the beginning
                    changeState("update", oldIndexer['jobID'], "wait_indexing", "", "","0")           

class CheckStateThread (threading.Thread):
    def __init__(self, executeTime, nextkeepAliveTime):
        threading.Thread.__init__(self)
        self.executeTime = executeTime
        self.nextkeepAliveTime = nextkeepAliveTime
    def run(self):
        while True:
            self.keepAliveTime = getExecuteTime()
            if self.keepAliveTime >= self.nextkeepAliveTime:
                self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
                checkIndexerState()

# Create new threads    
checkDBPerformace(MASTER_DB)
executeTime = getExecuteTime()
checkStateThread = CheckStateThread(executeTime,executeTime+KEEPALIVE_TIME_GAP)
# Start new Threads
checkStateThread.start()
while True:
    uniquePath = [] # read from configuration file node:path
    executeTime = getExecuteTime()
    if executeTime >= nextExecuteTime:
        nextExecuteTime = executeTime+EXECUTE_TIME_GAP
        triggerProcess("routine")
    
    
    

    
            

