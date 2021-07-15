import time
import subprocess
from datetime import date, datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()

cmd = "ps -eo pid,%mem,%cpu"

# Contains stat of per process
indexname = "os-stat-data"

# Contains stat for whole system
indexname2="os-stat-system-data"


def get_doc(pid, mem, cpu, now):
    return { # Creating dictionary of data
        "timestamp": now,
        "cpu": float(cpu),
        "mem": float(mem),
        "pid": pid
    }


def process_line(line):
    sp = line.split() # Splits line into columns
    now = datetime.now()
    return get_doc(sp[0], sp[1], sp[2], now)


while True:

    out = subprocess.getoutput(cmd) # Execution of command for retriving process stats
    lines = out.splitlines() # Splitting output by lines
    cpu = 0
    mem = 0
    for x in map(process_line, out.splitlines()[1:]):
        es.index(index=indexname, body=x) # Pushing data to elastisearch
        # print("Pushed") 
        cpu += x["cpu"]
        mem += x["mem"]
    es.index(index=indexname2, body={"mem": mem,
                                    "cpu": cpu, "timestamp": datetime.now()})

    time.sleep(1) # Waiting for a sec
