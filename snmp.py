import pymongo
MASTER_DB = "192.168.1.42"
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
while 1:
    checkDBPerformace(MASTER_DB)