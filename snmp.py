import subprocess
from subprocess import check_output
#status = subprocess.call("snmpwalk", shell=True)
SS_CPU_IDLE = ".1.3.6.1.4.1.2021.11.11.0"
MEM_AVAIL_REAL = ".1.3.6.1.4.1.2021.4.6.0"

output = check_output(["snmpwalk ", "-v", "2c", "-c", "allUser","-O" ,"e","192.168.1.42",SS_CPU_IDLE])
ssCpuIdle = (int)(output.split(" ")[3])

output = check_output(["snmpwalk ", "-v", "2c", "-c", "allUser","-O" ,"e","192.168.1.42",MEM_AVAIL_REAL])
memAvailReal = (int)(output.split(" ")[3])
#check_output()
print ssCpuIdle

print memAvailReal
