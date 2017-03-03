"""

    This file is for process deployment and debugging

"""

import os,sys
import subprocess
import time

# deploy datecenter processes on local machine
# set delay to change time inverval between deployment
def local_datacenter(config_filename, delay = 1):

    fin = open(config_filename)
    fin.readline().strip()
    for aline in fin:
        split_line = aline.strip().split()
        datacenter_id = int(split_line[0])
        subprocess.Popen("python datacenter.py %d %s" % (datacenter_id, config_filename), shell=True)
        time.sleep(delay)
    fin.close()

if __name__ == '__main__':
    
    local_datacenter("config.txt")


