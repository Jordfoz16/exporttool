#!/usr/bin/python3

# Written by Jim Apger, Cribl
#
# This is to be run on a Splunk Indexer for the purpose of exporting buckets and streaming their contents to Cribl Stream
#
# Example:  scribl.py -d /opt/splunk/var/lib/splunk/bots/db/ -r 34.220.39.122 -p 20000 -t -n4 -l /tmp/scribl.log -et 1564819155 -lt 1566429310#
#
# Required:
#  -d  Source directory containing the buckets.
#  -r  Remote address to send the exported data to.  This should be your Cribl worker with a TCP listener opened up.
#  -p  Remote TCP port to be used
# Optional:
#  -t  Send with TLS enabled
#  -n  Number of parallel stream to utilize
#  -l  Location to write/append the logging
#  -et Earliest epoch time for bucket selection
#  -lt Latest epoch time for bucket selection
#  -kv Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.  Can specify -kv multiple times
#
# Make sure nc (netcat) is in the path or hard code it below to fit your needs
#
# If you use the -kv option, make sure your pipeline in Cribl Stream accounts for the new field(s)

import argparse,os,subprocess,sys,time,logging,re
from multiprocessing import Pool

def getArgs(argv=None):
    parser = argparse.ArgumentParser(description="This is to be run on a Splunk Indexer for the purpose\
            of exporting buckets and streaming their contents to Cribl Stream")
    parser.add_argument("-t","--TLS", help="Send with TLS enabled", action='store_true')
    parser.add_argument("-n","--numstreams", default="1", type=int, help="Number of parallel stream to utilize")
    parser.add_argument("-l","--logfile", default="/tmp/SplunkToCribl.log", help="Location to write/append the logging")
    parser.add_argument("-et","--earliest", default=0, type=int, help="Earliest epoch time for bucket selection")
    parser.add_argument("-lt","--latest", default=9999999999, type=int, help="Latest epoch time for bucket selection")
    parser.add_argument("-kv","--keyval", action='append', nargs='+', help="Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.  Can specify -kv multiple times")
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("-d","--directory", help="Source directory containing the buckets", required=True)
    requiredNamed.add_argument("-r","--remoteIP", help="Remote address to send the exported data to", required=True)
    requiredNamed.add_argument("-p","--remotePort", help="Remote TCP port to be used", required=True)
    return parser.parse_args(argv)

def list_full_paths(directory,earliest,latest):
    dirs=os.listdir(directory)
    dirs=[x for x in dirs if x.startswith("db_")]   #keep only the dirs that we know contains buckets
    for dir in dirs:
        dirParsed=dir.split('_')
        maxEpoch=dirParsed[1]
        minEpoch=dirParsed[2]
        if not earliest < int(minEpoch) and latest > int(maxEpoch):
            dirs.remove(dir)
    return [os.path.join(directory, file) for file in dirs]

def buildCmdList(buckets,args):
    cliCommands=[]
    for bucket in buckets:
        exporttoolCmd="/opt/splunk/bin/splunk cmd exporttool "+bucket+" /dev/stdout -csv "
        if args.keyval:
            for pair in args.keyval:
                result = re.search(r"..(.*)=(.*)..", str(pair))
                kv="\""+result.group(1)+"::"+result.group(2)+"\""
                exporttoolCmd+="|sed -e 's/^\([[:digit:]]\{10\},\)\(.*\)/\\1"+kv+",\\2/'"
        exporttoolCmd+="| nc "
        if args.TLS:
            exporttoolCmd+="--ssl "
        exporttoolCmd+=args.remoteIP
        exporttoolCmd+=" "+args.remotePort
        print(exporttoolCmd)
        cliCommands.append(exporttoolCmd)
        logging.info("exporttoolCmd: %s ",exporttoolCmd)
    return cliCommands

def runCmd(cmd):
        startTime=time.time()
        p = subprocess.Popen(cmd,  shell=True, encoding='utf-8',stderr=subprocess.PIPE)
        while True:
            out = p.stderr.read(1)
            if out == '' and p.poll() != None:
                break
            if out != '':
                sys.stdout.write(out)
                sys.stdout.flush()
        logging.info("Finished in %s seconds: %s ",time.time()-startTime,cmd)

def getLogger(name):
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(name, 'a')
    logger.addHandler(handler)
    return logger

def main():
    argvals = None
    args = getArgs(argvals)
    global logging
    logging=getLogger(args.logfile)
    logging.info('\n------------\nStarting a new export using %i streams', args.numstreams)
    logging.info('Beginning Script with these args: %s',' '.join(f'{k}={v}' for k, v in vars(args).items()))
    startTime=time.time()
    buckets=(list_full_paths(args.directory,args.earliest,args.latest))
    if args.earliest < args.latest:
        logging.info('Search Min epoch = %i and Max epoch = %i',args.earliest,args.latest)
    else:
        logging.error('ERROR:  The specified Min epoch time (%i) must be less than the specified Max epoch time(%i)',args.earliest,args.latest)
        exit(1)
    logging.info('There are %s buckets in this directory that match the search criteria',len(buckets))
    logging.info('Exporting these buckets: %s',buckets)
    cliCommands=buildCmdList(buckets,args)
    with Pool(args.numstreams) as p:
        p.map(runCmd,cliCommands)
    logging.info('Done with script in %s seconds',time.time()-startTime)

if __name__ == "__main__":
    main()
