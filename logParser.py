import re, os, sys, socket, datetime, MySQLdb, time
import logging.handlers
from decimal import *


__author__ = 'mmj2286'


class Entry:
    def __init__(self, clid, type, hname, date, hr):
        self.key_ClientID = clid
        self.key_Service_Type = type
        self.key_HostName = hname
        self.key_Date = date
        self.key_Hour = hr
        self.ServerType = None
        self.DataCenter = None
        self.City = None
        self.DateInclude = None
        self.TotalMBs = 0
        self.TotalHits = 0
        self.TotalMissMBs = 0
        self.TotalMissHits = 0

    def printInfo(self):
        return 'ClientID:', self.key_ClientID, 'Type:', self.key_Service_Type, 'Hostname:', self.key_HostName, 'Date:', self.key_Date, \
            'Hour:', self.key_Hour, 'TotalMBs:', float(self.TotalMBs) / (float(1000) * float(1000)), \
            'TotalHits:', self.TotalHits, 'TotalMissMBs:', float(self.TotalMissMBs) / (float(1000) * float(1000)), 'TotalMissHits:', self.TotalMissHits

        # verify if is HIT, EXPIRED, UPDATING, STALE, MISS, -
    def compute(self, state, tmb):
        if state:
            if '\n' in state:
                state = state.replace('\n', '')

        self.TotalMBs += int(tmb)

        if state == 'HIT':
            self.TotalHits += 1
        elif state == 'MISS':
            self.TotalMissMBs += int(tmb)
            self.TotalMissHits += 1
        elif state == 'EXPIRED':
            self.TotalMissMBs += int(tmb)
            self.TotalMissHits += 1
        else:
            if state == 'STALE' or state == 'UPDATING' or state == '-':
                pass

    def equal(self, s):
        if self.key_ClientID == s:
            return True
        elif self.key_Service_Type == s:
            return True
        elif self.key_HostName == s:
            return True
        else:
            return False

def verifyLine(line):
    if line:
        spl = line.split("\t")
        if len(spl) == 13:
            return True
        else:
            return False
    else:
        return False

def createComputeAndAppendEntry(cid, tp, hname, dt, h, state, tmb):
    entry = Entry(cid, tp, hname, dt, h)
    entry.ServerType = ServerType
    entry.DataCenter = Datacenter
    entry.City = City
    entry.compute(state, tmb)
    ENTRIES.append(entry)


######## LOG CONF #############
logFolder = "logs/"
logFileSize = 10000000 #10MB in bytes
#number of log files before rotate
logRotate = 10
# loglevel: info, warning, error, critical, debug #KEEP MINIMUN LOG LEVEL critical for monitoring propose
loglevel = "debug"


def logMaker(logFile, logName = "Default"):
    # Set up a specific logger with our desired output level
    temp_logger = logging.getLogger(logName)

    if loglevel == 'info':
        temp_logger.setLevel(logging.INFO)
    elif loglevel == 'warning':
        temp_logger.setLevel(logging.WARNING)
    elif loglevel == 'error':
        temp_logger.setLevel(logging.ERROR)
    elif loglevel == 'critical':
        temp_logger.setLevel(logging.CRITICAL)
    elif loglevel == 'debug':
        temp_logger.setLevel(logging.DEBUG)
    else:
        temp_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(logFile, maxBytes=logFileSize, backupCount=logRotate)
    handler.setFormatter(formatter)
    temp_logger.addHandler(handler)
    temp_logger.info("Starting logging with level: " + loglevel)
    return temp_logger


#CRIADO O LOG

logFile = logFolder + "azionpurgeagent.log"
global logger
logger = logMaker(logFile, logName = "azionpurgeagent")


#get Type ==========================
Type = os.getcwd().split("/")[-1]
Type = Type[:10]
print 'running...', Type
#===================================

folder = os.listdir("./")

months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

getcontext().prec = 9

#get hostName
HostName = socket.gethostname()
HostName = HostName[:20]

#get ServerType, DataCenter, City
h1 = HostName.split('.')[0].split('-')
ServerType = h1[0]
Datacenter = h1[1]
City = h1[2]

def insert(self, db, table):
    cursor = db.cursor()
    #print table, self.key_Date, self.key_Hour, self.key_ClientID, Type, self.TotalMBs, self.TotalHits,
    #self.TotalMissMBs, self.TotalMissHits, HostName, ServerType, Datacenter, City, time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    # vals = table, self.key_Date, self.key_Hour, self.key_ClientID, Type, self.TotalMBs, self.TotalHits,
    # self.TotalMissMBs, self.TotalMissHits, HostName, ServerType, Datacenter, City, time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("INSERT INTO %s VALUES(%r, %r, %r, %r, %f, %r, %f, %r, %r, %r, %r, %r, %r)" % (table, self.key_Date, self.key_Hour, self.key_ClientID, Type, float(self.TotalMBs)/(float(1000) * float(1000)), self.TotalHits, float(self.TotalMissMBs)/(float(1000) * float(1000)), self.TotalMissHits, HostName, ServerType, Datacenter, City, time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())))

ENTRIES = []

start = datetime.datetime.now()

counter = 0

for file in folder:

    if file != os.path.basename(__file__) and re.match("(AZ.*)", file):

        #get clientId
        s2 = file.split('AZ.')[1].split('.log')
        clientId = s2[0]
        linecount = 0;

        with open(file, "r") as text:
            for line in text:
                if verifyLine(line=line):
                    spl = line.split("\t")
                    d = spl[4].split(":")

                    #date decoding

                    if '[' in d[0]:
                        d[0] = d[0].replace('[', '')

                    d01 = d[0].split("/")
                    dd = d01[0]
                    yy = d01[2]

                    if d01[1] in months:
                        mm = str(months.index(d01[1]) + 1)
                        if len(mm) == 1:
                            mm = '0' + mm

                    if len(dd) == 1:
                        dd = '0' + dd

                    Date = yy + '-' + mm + '-' + dd

                    if datetime.datetime.strptime(Date, '%Y-%m-%d'):
                        if not ENTRIES:
                            createComputeAndAppendEntry(clientId, Type, HostName, Date, d[1], spl[12], spl[8])
                        else:
                            ids = [e for e in ENTRIES if e.key_ClientID == clientId]
                            if ids:
                                types = [e for e in ids if e.key_Service_Type == Type]
                                if types:
                                    hostnanes = [e for e in types if e.key_HostName == HostName]
                                    if hostnanes:
                                        dates = [e for e in hostnanes if e.key_Date == Date]
                                        if dates:
                                            hours = [e for e in types if e.key_Hour == d[1]]
                                            if not hours:
                                                createComputeAndAppendEntry(clientId, Type, HostName, Date, d[1], spl[12], spl[8])
                                            else:
                                                hours[0].compute(spl[12], spl[8])
                                        else:
                                            createComputeAndAppendEntry(clientId, Type, HostName, Date, d[1], spl[12], spl[8])
                                    else:
                                        createComputeAndAppendEntry(clientId, Type, HostName, Date, d[1], spl[12], spl[8])
                                else:
                                    createComputeAndAppendEntry(clientId, Type, HostName, Date, d[1], spl[12], spl[8])
                            else:
                                createComputeAndAppendEntry(clientId, Type, HostName, Date, d[1], spl[12], spl[8])

                    #print line
                    counter += 1
                    linecount += 1
                else:
                    logger.error('FILE:', file, 'LINE:', linecount, ':', line)


print 'Elapsed Time:', datetime.datetime.now() - start
print counter, 'lines'

for e in ENTRIES:
    try:
        db = MySQLdb.connect('localhost', 'root', '123', 'parserDB')
        tableName = 'ClientsGlobalCounters'

        print 'Added:', e.printInfo()
        insert(e, db, tableName)
        db.commit()

    except MySQLdb.Error, e:
        db.rollback()
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

    finally:

        if db:
            db.close()





