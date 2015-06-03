import sys, datetime, time, threading, socket, string, random, pymongo
from pymongo import MongoClient
from subprocess import check_output
from operator import itemgetter
# lists containing alive process
# eg. 192.168.1.1:12345

def sleeper(seconds):
    time.sleep(seconds)

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  
def changeState(cmd, jobID, state, indexer, database, order, lastDoneRecord,lastFileName):
    # is called in case an error is found
    # insert state into MasterDB
    db = MASTER_DB_CONN.logsearch
    indexerStateCollection = db.indexer_state
    if cmd == "insert": 
        # for already indexed records
        if state == "wait_writing":
            document = {
                        'jobID':jobID,
                        'state':state,
                        'node':indexer,
                        'db_ip':database,
                        'lastDoneRecord':int(lastDoneRecord),
                        'lastFileName':"0",
                        'executionTime':int(time.time()) 
                        }
        # routine or startover (wait_indexing) case
        else:    
            order = extractCmd(order)
            if order[5] == 'multiLine':
                document = {
                            'jobID':jobID,
                            'state':state,
                            'indexer':indexer,
                            'db_ip':database,
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
                            'mmin':int(order[14]),
                            'interval':int(order[15]),
                            'lastFileName': order[16],
                            'lastDoneRecord':int(order[17]),
                            'executionTime':int(time.time())    
                            }
            elif order[5] == 'singleLine':
                document = {
                            'jobID':jobID,
                            'state':state,
                            'indexer':indexer,
                            'db_ip':database,
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
                            'mmin':int(order[12]),
                            'interval':int(order[13]),
                            'lastFileName':order[14],
                            'lastDoneRecord':int(order[15]),
                            'executionTime':int(time.time())     
                            }
        indexerStateCollection.insert(document)

    elif cmd == "update":
        if state == "indexing":
            indexerStateCollection.update({'jobID': jobID}, {"$set": {'state': state, 'indexer': indexer, 'db_ip':database}})
        #wait_indexing
        else:
            indexerStateCollection.update({'jobID': jobID}, {"$set": {'state': state, 'indexer': indexer, 'lastDoneRecord' : int(lastDoneRecord), 'lastFileName':lastFileName}})
        
    print "changeState"

def generateJobID(size=10, chars=string.ascii_uppercase + string.digits+string.ascii_lowercase):
    # generates jobID for tasks
    # return jobID
    print "generatejobID"
    return ''.join(random.choice(chars) for _ in range(size))

def getTask(mode):
    if mode == "routine":
        # Get tasks configured by users from MastDB
        taskCollection = retrieveCollection(MASTER_DB_CONN,'logsearch','service_config')
        # return all tasks in List
        return taskCollection.find({"$where": "this.lastExecutionTime+this.frequency <= "+ str(time.time())}) # dictionary type
    elif mode == "error":
        taskCollection = retrieveCollection(MASTER_DB_CONN,'logsearch','indexer_state')
        # return all tasks in List
        return taskCollection.find() # dictionary type
    print "getTask"
    
def retrieveCollection(conn,dbName,colName):
    # Get last record from state DB
    db = conn[dbName]
    collection = db[colName]
    return collection
  
# keepAlive is to test if each process is still alive
def checkIndexerState():
    
    IndexerStateCollection = retrieveCollection(MASTER_DB_CONN,'logsearch','indexer_state')
    deadIndexer = IndexerStateCollection.find({'state':'dead'})
    # if dead indexer exists 
    if deadIndexer.count() > 0:
        # Create new thread (ErrorRecoveryThread)
        errRecv = ErrorRecoveryThread()
        errRecv.start()
        print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] dead indexer(s) found"
    queingIndexer = IndexerStateCollection.find({'state':'wait_indexing'})
    if queingIndexer.count() > 0:
        # Create new threads (TriggerProcess error mode)
        triggerProcess('error')
        print 'queingIndexer exists'   
    
    # remove unknown task from StateDB
    taskList = []
    for indexer in IndexerStateCollection.find():
        taskList.append(indexer['jobID'])
    StateDBCollection = retrieveCollection(STATE_DB_CONN,'logsearch','StateDB_state')
    StateDBCollection.remove( { 'jobID': { '$nin': taskList } } )
    
           
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
    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] TriggerProcess: "+str(getExecuteTime())    
# rankProcess is to rank all processes by performance
def rankProcess(indexerList): 
    print "rankProcess"
    performance = []

    for indexer in indexerList:
        # SNMP to test CPU and memory
        # Get  % of CPU Idle
        try:
            output = check_output(["snmpwalk", "-v", "2c", "-c", COMMUNITY_STRING,"-O" ,"e",indexer['ip_addr'],SS_CPU_IDLE])
            ssCpuIdle = (int)(output.split(" ")[3])
            # Get Memory available size
            output = check_output(["snmpwalk", "-v", "2c", "-c", COMMUNITY_STRING,"-O" ,"e",indexer['ip_addr'],MEM_AVAIL_REAL])
            memAvailReal = (int)(output.split(" ")[3])
            indexerPerformance = {
                           'name': indexer['name'],
                           'ip_addr':indexer['ip_addr'],
                           'port':indexer['port'],
                           'db_port':indexer['db_port'],
                           'memory': memAvailReal,
                           'cpu': ssCpuIdle
                           }
            performance.append(indexerPerformance)
        except:
            print indexer['ip_addr']+" is not available"
    # sort indexers according to performance result
    indexerList = sorted(performance,key=itemgetter('cpu','memory'))

    return indexerList

    
def checkDBPerformace(host,port):
    # check DB workload
    output = check_output(["mongostat", "-host",host,"-port",str(port),"-n", "1"])
    insert = output.split('\n')
    # get first column of the result (insert rate)
    insertRate = insert[2][:6]

    return (int)(insertRate.translate(None, ' *'))
    
def getIndexer(): 
    # get indexers from MasterDB
    print "getIndexer"  
    indexerCollection = retrieveCollection(MASTER_DB_CONN,'logsearch','MasterDB_indexer')
    indexerList = []
    for indexer in indexerCollection.find():
        indexerDict = {
                       'id': indexer['_id'],
                       'name':indexer['name'],
                       'ip_addr':indexer['ip_addr'],
                       'port':indexer['port'],
                       'db_port':indexer['db_port']
                       }
        indexerList.append(indexerDict)
    return indexerList # dictionary type
     

class TriggerThread (threading.Thread):
    def __init__(self,tasks):
        self.process = None
        threading.Thread.__init__(self)
        self.tasks = tasks
    def run(self):
        if(self.tasks.count() > 0):
            print "###########################################################################"
            print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] Tasks exist"
            
            # Get all indexer
            indexerList = getIndexer()
            # rank all processes
            rankedIndexer = rankProcess(indexerList)
            # print indexerList
            execTimeList = []
            order = ""
            cmd = ""
            # tasks = getTask()
            # Iterate over ranked list and uniquePath and call sendTask(indexer,cmd)
            j=0;
            i=0;
            while (i < self.tasks.count() and len(rankedIndexer) > 0):
            #for i in range(0, self.tasks.count()):  
                # wait_indexing found
                # if making 3 attempts
                if j == len(rankedIndexer)*3:
                    break
                indexerIPAddr = rankedIndexer[(i+j)%len(rankedIndexer)]['ip_addr']
                indexerPort = rankedIndexer[(i+j)%len(rankedIndexer)]['port']
                if self.tasks[i]['state'] == 'wait_indexing':
                    # build cmd for indexer to run still missing the starting point (line number)
                    if self.tasks[i]['logType'] == 'singleLine':
                        cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+str(self.tasks[i]['mmin'])+'##'+str(self.tasks[i]['interval'])+'##'+self.tasks[i]['lastFileName']+'##'+str(self.tasks[i]['lastDoneRecord'])
                    elif self.tasks[i]['logType'] == 'multiLine':
                        cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['logStartTag']+"##"+self.tasks[i]['logEndTag']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+str(self.tasks[i]['mmin'])+'##'+str(self.tasks[i]['interval'])+'##'+self.tasks[i]['lastFileName']+'##'+str(self.tasks[i]['lastDoneRecord'])
                    jobId = self.tasks[i]['jobID']
                    stateDB = STATE_DB+":"+str(STATE_DB_PORT)
                    actualDB = INDEXED_DB+":"+str(INDEXED_DB_PORT)
                    order = "indexing##"+jobId+"##"+stateDB+"##"+cmd+"##"+actualDB
                    print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] continuing indexing task: "+jobId
                    
                    server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
                    #infinite loop so that function do not terminate and thread do not end.
                    try:  
                        server.connect ( ( indexerIPAddr, int(indexerPort) ) )
                        server.send (order)
                        server.close()    
                        # call changeState to update on MasterDB (indexer_state)
                        changeState("update", jobId, "indexing", rankedIndexer[(i+j)%len(rankedIndexer)]['name'], rankedIndexer[(i+j)%len(rankedIndexer)]['ip_addr'],"","","")   
                    except socket.error:
                        j+=1
                        i-=1
                        print "error: indexer-"+rankedIndexer[(i+j)%len(rankedIndexer)]['name']+" is not ready"
                        server.close()

                # routing task
                elif self.tasks[i]['state'] == 'routine':
                    # build cmd for indexer to run still missing the starting point (line number)
                    if self.tasks[i]['logType'] == 'singleLine':
                        cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+str(self.tasks[i]['mmin'])+'##'+str(self.tasks[i]['interval'])+'##'+self.tasks[i]['lastFileName']+'##'+str(self.tasks[i]['lastDoneRecord'])
                    elif self.tasks[i]['logType'] == 'multiLine':
                        cmd = self.tasks[i]['service']+"##"+self.tasks[i]['system']+"##"+self.tasks[i]['node']+"##"+self.tasks[i]['process']+"##"+self.tasks[i]['path']+"##"+self.tasks[i]['logType']+"##"+self.tasks[i]['logStartTag']+"##"+self.tasks[i]['logEndTag']+"##"+self.tasks[i]['msisdnRegex']+"##"+self.tasks[i]['dateHolder']+"##"+self.tasks[i]['dateRegex']+"##"+self.tasks[i]['dateFormat']+"##"+self.tasks[i]['timeRegex']+"##"+self.tasks[i]['timeFormat']+'##'+str(self.tasks[i]['mmin'])+'##'+str(self.tasks[i]['interval'])+'##'+self.tasks[i]['lastFileName']+'##'+str(self.tasks[i]['lastDoneRecord'])
                        
                    # generate JobID
                    jobId = generateJobID()
                    # assign jobID to each node
                    stateDB = STATE_DB+":"+str(STATE_DB_PORT)
                    actualDB = INDEXED_DB+":"+str(INDEXED_DB_PORT)
                    order = "indexing##"+jobId+"##"+stateDB+"##"+cmd+"##"+actualDB
                    # print "indexing"
                    # print order
                    # print rankedIndexer[i%len(rankedIndexer)]['name']+"-"+jobId 
                    server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
                    #infinite loop so that function do not terminate and thread do not end.
                    try:  
                        server.connect ( ( indexerIPAddr, int(indexerPort) ) )
                        server.send (order)
                        server.close()    
                        # insert task into state DB
                        stateCollection = retrieveCollection(STATE_DB_CONN,'logsearch','StateDB_state')
                        stateCollection.insert({ 
                                    "jobID": jobId,
                               "state": "indexing",
                                "lastFileName": "",
                             "lastDoneRecord": "-1",
                              "db_ip": indexerIPAddr
                               })
                        # call changeState to add state on MasterDB
                        changeState("insert", jobId, "indexing", rankedIndexer[(i+j)%len(rankedIndexer)]['name'], rankedIndexer[(i+j)%len(rankedIndexer)]['ip_addr'],cmd,"-1","")
                        execTimeDict = {
                                    '_id': self.tasks[i]['_id'],
                                    'lastExecutionTime':int(time.time())
                                    }
                        execTimeList.append(execTimeDict)
                    except socket.error:
                        j+=1
                        i-=1
                        print "error: indexer-"+rankedIndexer[(i+j)%len(rankedIndexer)]['name']+" is not ready"
                        server.close()
                i+=1  
                print "["+datetime.datetime.fromtimestamp(int(getExecuteTime())).strftime('%Y-%m-%d %H:%M:%S')+"] working indexer is : "+rankedIndexer[(i+j)%len(rankedIndexer)]['name']+" for jobID: "+jobId   
            # updateExecutionTime
            for i in range(0, len(execTimeList)): 
                db = MASTER_DB_CONN.logsearch
                serviceConfigCollection = db.service_config
                serviceConfigCollection.update({'_id': execTimeList[i]['_id']}, {"$set": {'lastExecutionTime': execTimeList[i]['lastExecutionTime']}})
            #print cmd
            print "###########################################################################"

class ErrorRecoveryThread (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        print "ErrorRecoveryThread"
        # get DeadIndexer
        indexerStateCollection = retrieveCollection(MASTER_DB_CONN,'logsearch','indexer_state')
        deadIndexerCount = indexerStateCollection.find({'state':'dead'})
        stateDBCollection = retrieveCollection(STATE_DB_CONN,'logsearch','StateDB_state')
        indexerDetailCollection = retrieveCollection(MASTER_DB_CONN,'logsearch','MasterDB_indexer')

        for i in range(0, deadIndexerCount.count()):
            # get before-dead state
            deadIndexer = indexerStateCollection.find_one({'state':'dead'})
            oldIndexer = stateDBCollection.find_one({'jobID':deadIndexer['jobID']})   
            ### check StateDB for the dead indexer then set ignore bit = 1 for this record
            # Get Dead Idx detail
            deadIdxDetail = indexerDetailCollection.find_one({'name':deadIndexer['indexer']})  
            # if stateDB shows "indexing"
            if oldIndexer['state'] == "indexing":           
                # if local DB of the dead indexer is alive

                # Creat new job ID
                #jobId = generateJobID()
                if deadIndexer['logType'] == 'singleLine':
                    order = deadIndexer[i]['service']+"##"+deadIndexer['system']+"##"+deadIndexer['node']+"##"+deadIndexer['process']+"##"+deadIndexer['path']+"##"+deadIndexer['logType']+"##"+deadIndexer['msisdnRegex']+"##"+deadIndexer['dateHolder']+"##"+deadIndexer['dateRegex']+"##"+deadIndexer['dateFormat']+"##"+deadIndexer['timeRegex']+"##"+deadIndexer['timeFormat']+'##'+str(deadIndexer['mmin'])+'##'+str(deadIndexer['interval'])+'##'+oldIndexer['lastFileName']+'##'+str(oldIndexer['lastDoneRecord'])
                elif deadIndexer['logType'] == 'multiLine':
                    order = deadIndexer['service']+"##"+deadIndexer['system']+"##"+deadIndexer['node']+"##"+deadIndexer['process']+"##"+deadIndexer['path']+"##"+deadIndexer['logType']+"##"+deadIndexer['logStartTag']+"##"+deadIndexer['logEndTag']+"##"+deadIndexer['msisdnRegex']+"##"+deadIndexer['dateHolder']+"##"+deadIndexer['dateRegex']+"##"+deadIndexer['dateFormat']+"##"+deadIndexer['timeRegex']+"##"+deadIndexer['timeFormat']+'##'+str(deadIndexer['mmin'])+'##'+str(deadIndexer['interval'])+'##'+oldIndexer['lastFileName']+'##'+str(oldIndexer['lastDoneRecord'])
                  
                # for non-indexed records
                changeState("update", oldIndexer['jobID'], "wait_indexing", "", "",order,oldIndexer['lastDoneRecord'],oldIndexer['lastFileName'])        

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
            checkIndexerState()



# checkDBPerformace(MAIN_DB,MAIN_DB_PORT)
# CONSTANT
EXECUTE_TIME_GAP = 5; #seconds
KEEPALIVE_TIME_GAP = 2; #seconds
SS_CPU_IDLE = ".1.3.6.1.4.1.2021.11.11.0"
MEM_AVAIL_REAL = ".1.3.6.1.4.1.2021.4.6.0"
COMMUNITY_STRING= "allUser"
now=datetime.datetime.now()
NEXT_EXECUTION_TIME = time.mktime(now.timetuple())+5
NEXT_KEEPALIVE_TIME = time.mktime(now.timetuple())+KEEPALIVE_TIME_GAP
MASTER_DB = sys.argv[1]
MASTER_DB_PORT = int(sys.argv[2])
MASTER_DB_CONN = MongoClient(MASTER_DB, MASTER_DB_PORT)
databaseCollection = retrieveCollection(MASTER_DB_CONN,"logsearch","Database_config")
STATE_DB = databaseCollection.find_one({'name':'State_DB'})['ip_addr']
STATE_DB_PORT = databaseCollection.find_one({'name':'State_DB'})['port']
STATE_DB_CONN = MongoClient(STATE_DB, STATE_DB_PORT) 
INDEXED_DB = databaseCollection.find_one({'name':'Indexed_DB'})['ip_addr']
INDEXED_DB_PORT = databaseCollection.find_one({'name':'Indexed_DB'})['port']
#localportCollection = retrieveCollection(MASTER_DB_CONN,"logsearch","MasterDB_Indexer")
#LOCAL_DB_PORT = databaseCollection.find_one({'name':'Indexed_DB'})['db_port']
# Create new threads
executeTime = getExecuteTime()
#checkStateThread = CheckStateThread(executeTime,executeTime+KEEPALIVE_TIME_GAP)
checkStateThread = CheckStateThread()
# Start new Threads
checkStateThread.start()
while True:
    #uniquePath = [] # read from configuration file node:path
    #executeTime = getExecuteTime()
    #if executeTime >= NEXT_EXECUTION_TIME:
    #    NEXT_EXECUTION_TIME = executeTime+EXECUTE_TIME_GAP
    sleeper(EXECUTE_TIME_GAP)
    triggerProcess("routine")
    
    
    

    
            

