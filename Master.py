import datetime, time, threading
# lists containing alive process
# eg. 192.168.1.1:12345
# CONSTANT
EXECUTE_TIME_GAP = 5; #seconds
KEEPALIVE_TIME_GAP = 2; #seconds
processList = [("127.0.0.1","12345")]
aliveList = []
rankedList = []
uniquePath = []
now=datetime.datetime.now()
nextExecuteTime = time.mktime(now.timetuple())+EXECUTE_TIME_GAP
nextkeepAliveTime = time.mktime(now.timetuple())+KEEPALIVE_TIME_GAP

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  

# keepAlive is to test if each process is still alive
def keepAlive():
    # aliveList = Query from working DB
    # if a working process is dead
    # then reassign task to another process 
    
    # if found dead
    # Create new threads
    # reassignThread = ReassignTaskThread("Task")
    # Start new Threads
    # reassignThread.start()
    print "keepAlive"
 
 
# TriggerProcess is to trigger process to work
def TriggerProcess(aliveList):
    # rank all processes
    # rankedLists = rankProcess(workingLists);
    # Iterate ranked list and uniquePath to assign tasks
    print "TriggerProcess: "+str(nextExecuteTime)    
# rankProcess is to rank all processes by performance
def rankProcess(aliveList): 
    # SNMP to test CPU and memory
    # reorder ranked process
    # return 
    print "rankProcess"
    
# assignTask is to assign tasks to processes
def assignTask(process,cmd): 
    # SNMP to test CPU and memory
    # reorder ranked process
    # update workingProcess DB setting state as alive
    # return 
    print "assignTask"    

def getHost(process): 
    # separate process (host:port)
    # report host
    print "getHost"  
    
def getPort(process): 
    # separate process (host:port)
    # report port
    print "getPort"   

class ReassignTaskThread (threading.Thread):
    def __init__(self, task):
        threading.Thread.__init__(self)
        self.task = task
    def run(self):
        assignTask(self.task,self.task)


class KeepAliveThread (threading.Thread):
    def __init__(self, executeTime, nextkeepAliveTime):
        threading.Thread.__init__(self)
        self.executeTime = executeTime
        self.nextkeepAliveTime = nextkeepAliveTime
    def run(self):
        while True:
            self.keepAliveTime = getExecuteTime()
            if self.keepAliveTime >= self.nextkeepAliveTime:
                self.nextkeepAliveTime = self.keepAliveTime+KEEPALIVE_TIME_GAP
                aliveList = keepAlive()

# Create new threads
executeTime = getExecuteTime()
keepAliveThread = KeepAliveThread(executeTime,executeTime+KEEPALIVE_TIME_GAP)
# Start new Threads
keepAliveThread.start()

while True:
    uniquePath = [] # read from configuration file node:path
    executeTime = getExecuteTime()
    if executeTime >= nextExecuteTime:
        nextExecuteTime = executeTime+EXECUTE_TIME_GAP
        TriggerProcess(aliveList)
    
    
    

    
            

