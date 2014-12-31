import socket, datetime, time
import sys
from thread import *
 
HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port
KEEPALIVE_TIME_GAP = 2; #seconds
TIMEOUT = 5
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

def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  

#Function for handling connections. This will be used to create threads
def clientThread(conn,addr):
    #Sending message to connected client
    conn.send('Welcome to the server. Type something and hit enter\n') #send only takes string
    print addr[0]+":"+str(addr[1])
    #infinite loop so that function do not terminate and thread do not end.
    while True:
         
        # Receiving from client
        # Listening for Keep Alive Status
        try:
            data = conn.recv(1024)
            reply = 'OK...' + data
            #if not data: 
            #    break
     
            #conn.sendall(reply)
        except socket.timeout:
            # update workingProcessDB setting state as dead 
            print "timeout"
            break;
            #Timeout occurred, do things
        #if data == "done"
                 
    #came out of loop
    conn.close()
 
#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    conn.settimeout(TIMEOUT)
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
     
    #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientThread ,(conn,addr,))
 
s.close()