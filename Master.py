import datetime, time, threading, socket, string, random, pymongo
from pymongo import MongoClient
# lists containing alive process
# eg. 192.168.1.1:12345
# CONSTANT
EXECUTE_TIME_GAP = 5; #seconds
KEEPALIVE_TIME_GAP = 2; #seconds
MASTER_DB = "192.168.1.38"
MASTER_DB_PORT = 27017
processList = [("127.0.0.1","9999")]
aliveList = []
rankedList = []
uniquePath = []
now=datetime.datetime.now()
nextExecuteTime = time.mktime(now.timetuple())+EXECUTE_TIME_GAP
nextkeepAliveTime = time.mktime(now.timetuple())+KEEPALIVE_TIME_GAP

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  

def changeState(cmd, jobID, state, node, dbNode, order):
    # is called in case an error is found
    # insert state into MasterDB
    mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    db = mongoClient.logsearch
    IndexerStateCollection = db.indexer_state
    document = {
                'jobID':jobID,
                'state':state,
                'node':node,
                'dbNode':dbNode,
                'order':order
                }
    if cmd == "insert":
        IndexerStateCollection.insert(document)
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
    if mode == "routine":
        # Get tasks configured by users from MastDB
        mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
        db = mongoClient.logsearch
        taskCollection = db.service_config
    elif mode == "error":
        mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
        db = mongoClient.logsearch
        taskCollection = db.indexer_state
    # return all tasks in List
    return taskCollection.find() # dictionary type
    print "getTask"

def getRecordFromStateDB(jobID):
    # Get last record from state DB
    # return string containing jobID:state:last_record:node
    print "getRecordStateDB"
    
# keepAlive is to test if each process is still alive
def checkIndexerState():
    mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    db = mongoClient.logsearch
    IndexerStateCollection = db.indexer_state
    deadIndexer = IndexerStateCollection.find({'state':'dead'})
    if deadIndexer.count() > 0:
        # Create new thread (ErrorRecoveryThread)
        print 'dead exists'
    queingIndexer = IndexerStateCollection.find({'state':'wait_indexing'})
    if queingIndexer.count() > 0:
        # Create new threads (TriggerProcess error mode)
        print 'queingIndexer exists'
    queingWriter = IndexerStateCollection.find({'state':'wait_writing'})
    if queingWriter.count() > 0:
        # Create new thread (WritingThread)   
        print 'queingWriter exists'              
    print "checkIndexerState"
 
# TriggerProcess is to trigger process to work
def TriggerProcess(mode):
    tasks = getTask(mode)
    triggerProcess = TriggerThread( "127.0.0.1",9990,tasks)
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
    
def checkDBPerformace():
    # check DB workload
    # return T of F
    #mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    #db = mongoClient.logsearch
    #print db.command("collstats", "indexer_state")
    #print db.command("currentOp")
    conn = pymongo.connection.Connection(MASTER_DB, 27017)
    all_ops = conn['admin']['$cmd.sys.inprog'].find_one('inprog')['inprog']
    active_ops = [op for op in all_ops if op['active']]
 
    print '%d/%d active operations' % (len(active_ops), len(all_ops))
    print "checkDBPerformace"
    
def getHost(process): 
    # separate process (host:port)
    # report host
    print "getHost"  
    
def getPort(process): 
    # separate process (host:port)
    # report port
    print "getPort"   
    
def getIndexer(): 
    # get indexers from MasterDB
    print "getIndexer"  
    mongoClient = MongoClient(MASTER_DB, MASTER_DB_PORT)
    db = mongoClient.logsearch
    indexerCollection = db.MasterDB_indexer
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
    def __init__(self,host,port,tasks):
        self.process = None
        threading.Thread.__init__(self)
        self.host = "127.0.0.1"
        self.port = 9990
        self.tasks = tasks
    def run(self):
        # Get all indexer
        indexerList = getIndexer()
        # rank all processes
        rankedIndexer = rankProcess(indexerList)
        print indexerList
        # tasks = getTask()
        # Iterate over ranked list and uniquePath and call sendTask(indexer,cmd)
        for i in range(0, self.tasks.count()):
            # build cmd for indexer to run still missing the starting point (line number)
            cmd = "sudo -u logsearch python indexScript.py test "+self.tasks[i]['path']+" "+self.tasks[i]['logType']+" "+self.tasks[i]['logStartTag']+" "+self.tasks[i]['logEndTag']+" "+self.tasks[i]['msisdnRegex']+" "+self.tasks[i]['dateHolder']+" "+self.tasks[i]['dateRegex']+" "+self.tasks[i]['dateFormat']+" "+self.tasks[i]['timeRegex']+" "+self.tasks[i]['timeFormat']
            # generate JobID
            jobId = generateJobID()
            # assign jobID to each node
            order = "indexing#"+jobId+"#"+cmd
            print order
            print rankedIndexer[i%len(rankedIndexer)]['name']+"-"+jobId 
            # call changeState to add state on MasterDB
            changeState("insert", jobId, "indexing", rankedIndexer[i%len(rankedIndexer)]['name'], "",cmd)
            # send tasks to indexers
            sendTask(self.host,self.port,order)
        #server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        #server.connect ( ( self.host, self.port ) )
        #infinite loop so that function do not terminate and thread do not end.
        #try:
        #    server.send (order)
        #    server.close()       
        #except socket.error:
        #    #came out of loop
        #    server.close()

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
    def __init__(self, task):
        threading.Thread.__init__(self)
        self.task = task
    def run(self):
        print "ErrorRecoveryThread"
        # check StateDB for the dead indexer then set ignore bit = 1 for this record
        # if "indexing"
            # if local DB of the dead indexer is down
                # start over from the beginning
                # Call addTask(wait_indexing, node,path,"",indexing) to add Task to MasterDB
            # else
                # get the last indexed record of the dead indexer from stateDB
                # Call addTask(wait_writing, indexer's local_DB) to add Task to MasterDB
                # Call addTask(wait_indexing, node,path,last recorded,indexing) to add Task to MasterDB
        # if "writing" 
            # if local DB of the dead indexer is down
                # start over from the beginning
                # Call addTask(wait_writing, "") to add Task to MasterDB  
            # else
                # get the last written record of the dead indexer from stateDB
                # Call addTask(wait_writing, indexer's local_DB) to add Task to MasterDB       


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
                aliveList = checkIndexerState()

# Create new threads    
checkDBPerformace()
executeTime = getExecuteTime()
checkStateThread = CheckStateThread(executeTime,executeTime+KEEPALIVE_TIME_GAP)
# Start new Threads
checkStateThread.start()
while True:
    uniquePath = [] # read from configuration file node:path
    executeTime = getExecuteTime()
    if executeTime >= nextExecuteTime:
        nextExecuteTime = executeTime+EXECUTE_TIME_GAP
        TriggerProcess("routine")
    
    
    

    
            

