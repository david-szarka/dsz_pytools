import psutil
from datetime import datetime

def match_needed_proc(matchlist: list):
    resultlist =[]
    proclist = psutil.process_iter()
    for proc in proclist:
        try:
            for needproc in matchlist:
                if needproc == proc.name():
                    resultlist.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return resultlist


def match_needed_proc_in(matchlist: list):
    resultlist =[]
    proclist = psutil.process_iter()
    for proc in proclist:
        try:
            for needproc in matchlist:
                if needproc in proc.name():
                    resultlist.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return resultlist


def get_last_created_processes(processlist):
    resultdict = dict()
    for proc in processlist:
        prc_name = proc.name()
        if resultdict.get(prc_name):
            crt_time = proc.create_time()
            if resultdict.get(prc_name).create_time() < crt_time:
                resultdict[prc_name] = proc
        else:
            resultdict[prc_name] = proc
    return resultdict


def get_last_created_process(processlist):
    result = None
    for proc in processlist:
        prc_name = proc.name()
        if result:
            crt_time = proc.create_time()
            if result.create_time() < crt_time:
                result = proc
        else:
            result = proc
    return result


def get_oldest_created_processes(processlist):
    resultdict = dict()
    for proc in processlist:
        prc_name = proc.name()
        if resultdict.get(prc_name):
            crt_time = proc.create_time()
            if resultdict.get(prc_name).create_time() > crt_time:
                resultdict[prc_name] = proc
        else:
            resultdict[prc_name] = proc
    return resultdict


def get_oldest_created_process(processlist):
    result = None
    for proc in processlist:
        prc_name = proc.name()
        if result:
            crt_time = proc.create_time()
            if result.create_time() > crt_time:
                result = proc
        else:
            result = proc
    return result


def get_datetime_from_timestamp(tmstmp):
    return datetime.fromtimestamp(tmstmp).strftime("%Y-%m-%d %H:%M:%S")


def find_last_procdatetime(matchlist):
    neededprocesses = match_needed_proc(matchlist)
    if neededprocesses:
        oldestproces = get_oldest_created_process(neededprocesses)
        oldesttime = get_datetime_from_timestamp(oldestproces.create_time())
        return oldesttime
    else:
        return None


if __name__ == "__main__":
    print(match_needed_proc(["notep","firefox.exe"]))
    print("not exact",match_needed_proc_in(["notep","fox.exe"]))
    print(get_last_created_processes(match_needed_proc(["notepad.exe","firefox.exe"])))
    print(get_last_created_process(match_needed_proc(["notepad.exe","firefox.exe"])))
    print(find_last_procdatetime(["notepad.exe","firefox.exe"]))
    
    # Iterate over all running process
    """for proc in psutil.process_iter():
        try:
            # Get process name & pid from process object.
            processName = proc.name()
            processID = proc.pid
            print(processName , ' ::: ', processID)
            print(get_datetime_from_timestamp(proc.create_time()))
            
            #print(proc.create_time())
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass"""
    
    print(get_oldest_created_processes(match_needed_proc(["notepad.exe","firefox.exe"])))
    print(get_oldest_created_process(match_needed_proc(["notepad.exe","firefox.exe"])))


