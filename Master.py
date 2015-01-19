import datetime, time, threading, socket
# lists containing alive process
# eg. 192.168.1.1:12345
# CONSTANT
EXECUTE_TIME_GAP = 5; #seconds
KEEPALIVE_TIME_GAP = 2; #seconds
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

def addTask(state, node, path, lastIndexedRecord, dbNode):
    # is called in case an error is found
    # insert state into MasterDB
    print "addTask"
     
def getTask():
    # Get tasks configured by users from MastDB
    # return all tasks in List
    print "getTask"
    
# keepAlive is to test if each process is still alive
def checkIndexerState():
    # aliveList = Query from working DB
    # if a working process is dead
    # then reassign task to another process 
    
    # if found dead
        # Create new thread (ErrorRecoveryThread)
    # if found wait_writing
        # Create new thread (WritingThread)           
    # if found wait_indexing 
        # Create new threads (TriggerProcess)
               
    print "checkIndexerState"
 
# TriggerProcess is to trigger process to work
def TriggerProcess(aliveList):
    triggerProcess = TriggerThread( "127.0.0.1",9990  )
    triggerProcess.start()
    #triggerProcess.join()
    print "TriggerProcess: "+str(nextExecuteTime)    
# rankProcess is to rank all processes by performance
def rankProcess(aliveList): 
    # SNMP to test CPU and memory
    # reorder ranked process
    # return 
    print "rankProcess"
    
# assignTask is to assign tasks to processes
def sendTask(indexer,cmd): 
    # send cmd to the specified indexer
    # update MasterDB setting state as "indexing"
    # return 
    print "sendTask"     
    
def checkDBPerformace():
    # check DB workload
    # return T of F
    print "checkDBPerformace"
    
def getHost(process): 
    # separate process (host:port)
    # report host
    print "getHost"  
    
def getPort(process): 
    # separate process (host:port)
    # report port
    print "getPort"   

class TriggerThread (threading.Thread):
    def __init__(self,host,port):
        self.process = None
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
    def run(self):
        # rank all processes
        # rankedLists = rankProcess(workingLists);
        # Iterate ranked list and uniquePath and call sendTask(indexer,cmd)
        server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        server.connect ( ( self.host, self.port ) )
        #infinite loop so that function do not terminate and thread do not end.
        try:
            server.send ('indexing')
            server.close()       
        except socket.error:
            #came out of loop
            server.close()

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
executeTime = getExecuteTime()
checkStateThread = CheckStateThread(executeTime,executeTime+KEEPALIVE_TIME_GAP)
# Start new Threads
checkStateThread.start()
TriggerProcess(aliveList)
while True:
    uniquePath = [] # read from configuration file node:path
    executeTime = getExecuteTime()
    if executeTime >= nextExecuteTime:
        nextExecuteTime = executeTime+EXECUTE_TIME_GAP
        TriggerProcess(aliveList)
    
    
    

    
            

