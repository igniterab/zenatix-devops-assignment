import time
import subprocess
from datetime import date, datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()

cmd = "ps -eo pid,%mem,%cpu"
indexname = "os-stat-data"
indexname2="os-stat-system-data"


def get_doc(pid, mem, cpu, now):
    return {
        "timestamp": now,
        "cpu": float(cpu),
        "mem": float(mem),
        "pid": pid
    }


def process_line(line):
    sp = line.split()
    now = datetime.now()
    return get_doc(sp[0], sp[1], sp[2], now)


while True:

    out = subprocess.getoutput(cmd)
    lines = out.splitlines()
    cpu = 0
    mem = 0
    for x in map(process_line, out.splitlines()[1:]):
        es.index(index=indexname, body=x)
        cpu += x["cpu"]
        mem += x["mem"]
    es.index(index=indexname2, body={"mem": mem,
                                    "cpu": cpu, "timestamp": datetime.now()})

    time.sleep(1)

