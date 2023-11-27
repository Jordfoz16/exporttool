#!/usr/bin/python3

# Written by Jim Apger, Cribl
#
# This is to be run on a Splunk Indexer for the purpose of exporting buckets and streaming their contents to Cribl Stream
#
# Features:
#   traditional, Smart Store, and frozen buckets
#   skips cluster replicated and hot buckets
#
# Example:  exporttool.py -d /opt/splunk/var/lib/splunk/bots/ -r 34.220.39.122 -p 20000 -t -n4 -l /tmp/scribl.log -et 1564819155 -lt 1566429310#
#
# Example:  exporttool.py -d /opt/splunk/var/lib/splunk/bots/ -r 34.220.39.122 -p 20000 -l /tmp/scribl.log -kv index=test
#
# Example:  exporttool.py -i -d buckets.txt -r 34.220.39.122 -p 20000 -l /tmp/scribl.log
#
# Required:
#  --directory  Source directory pointing at the index.
#  --dest_host  Remote address to send the exported data to.  This should be your Cribl worker with a TCP listener opened up.
#  --dest_port  Remote TCP port to be used
# Optional:
#  --tls  Send with TLS enabled
#  --num_streams  Number of parallel stream to utilize
#  --logfile  Location to write/append the logging
#  --earliest Earliest epoch time for bucket selection
#  --latest Latest epoch time for bucket selection
#  --keyval Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.  Can specify -kv multiple times.
#  --bucket_name  Add the bucket=<bucket_name> kv pair to the output
#  -i  Import buckets from a file rather than crawl the provided directory
#
# If you use the --keyval option, make sure your pipeline in Cribl Stream accounts for the new field(s)
#
#
# What's New?
#
# Version 2.0.0 (Dec 2022) - Apger
#   - The -d option now needs to point at the directory containing the index name instead of the bucket (from 1.0.0)
#   - Splunk SmartStore is now supported
# Version 2.0.1 (Jan 2023) - Apger
#   - skips hot buckets
#   - fixed the filtering expression for selecting buckets based on earliest/latest time
# Version 2.0.2 (Mar 2023) - Apger
#   - Add the -b arg
# Version 2.0.3 (Jun 2023) - Apger
#   - update earliest/latest search
# Version 2.0.4 (Sept 2023) - Apger
#   - add the ability to import bucket list from file
# Version 2.1.0 (November 2023) - Brant
#   - added native libraries to eliminate the requirement for netcat
#   - added the ability to specify options in a configuration file (can be overridden on CLI)


import argparse
import tomllib
import glob
from subprocess import Popen, PIPE, STDOUT
# import io
import time
import logging
import re
import datetime
import socket
import ssl
from multiprocessing import Pool

def get_args(argv=None):
    with open("config.toml", "rb") as con:
        config = tomllib.load(con)
    parser = argparse.ArgumentParser(description="This is to be run on a Splunk Indexer for the purpose\
            of exporting buckets and streaming their contents to tcp output")
    parser.add_argument("--tls", help="Send with TLS enabled", action='store_true')
    parser.add_argument("--import_buckets", help="Import buckets from a file (specifid by the -d arg) rather than crawl the provided directory", action='store_true')
    parser.add_argument("--num_streams", type=int, help="Number of parallel streams to utilize")
    parser.add_argument("--logfile", help="Location to write/append the logging")
    parser.add_argument("--earliest", type=int, help="Earliest epoch time for bucket selection")
    parser.add_argument("--latest", type=int, help="Latest epoch time for bucket selection")
    parser.add_argument("--keyval", action='append', nargs='+', help="Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.  Can specify -kv multiple times")
    parser.add_argument("--bucket_name", help="Add bucket=<bucket_name> to the output",action='store_true')
    parser.add_argument("--directory", default=config['directory'], help="Source directory pointing at the index")
    parser.add_argument("--dest_host", default=config['dest_host'], help="Remote address to send the exported data to")
    parser.add_argument("--dest_port", default=config['dest_port'], help="Remote TCP port to be used")
    parser.set_defaults(tls=config['tls'],\
                        import_buckets=config['import_buckets'],\
                        num_streams=config['num_streams'],\
                        logfile=config['logfile'],\
                        earliest=config['earliest'],\
                        latest=config['latest'],\
                        keyval=config['keyval'],\
                        bucket_name=config['bucket_name'])
    return parser.parse_args(argv)

def list_full_paths(directory,earliest,latest,import_buckets):
    #  We are accounting for directory structures specific to traditional on-prem and Smart Store
    #  Use the -i arg is you are mounting a SmartStore directory (speed) or importing individual buckets
    #  The one thing common to them that contains min/maw epoch times is the .tsidx file
    #  Smart Store dir example:  _internal/db/bd/e3/14~676B2388-3181-4A73-BD1E-43F02EF050B4/guidSplunk-676B2388-3181-4A73-BD1E-43F02EF050B4/1668952678-1668520680-9018843933635107078.tsidx
    #  Traditional dir example:  _internaldb/db/db_1674422056_1673990057_8/1674309022-1673990057-8288841824203874392.tsidx
    files = []
    buckets = []
    if len(import_buckets) > 0:
        for line in import_buckets:
            files.append(line.strip())
    else:
        files_to_process = glob.glob(f"{directory}/**/*.tsidx", recursive=True)
        files = files_to_process

    for file in files:
        tsidx=file.split('/')[-1] # the tsidx filename less the path
        max_epoch=tsidx.split('-')[-3] # Grab max and min from file name
        min_epoch=tsidx.split('-')[-2] # Grab max and min from file name
        bucketName=file.split("/")[-2] #Grab the name of the bucket (parent dir for the tsidx filename)
        if earliest <= int(max_epoch) and latest >= int(min_epoch) and "DISABLED" not in file and not bucketName.startswith("rb_"):  # filter buckets if user passed min/max epoch times
        # Will assume everything is a bucket except for dir names that contain DISABLED, are cluster associated replicated buckets (tested for non-smartstore), or hot buckets
        # For an on-prem config, we might find multiple tsidx files in an index.  Only grab the iunique parent directory containing these tsidx files once.
            buckets.append(re.sub('\/[^\/]+$', '', file))  #strip the .tsidx filename from the path to only include the parent dir
    return(buckets)


def run_cmd_send_data(command):

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        print(f"Error creating socket: {e}")
        exit(1)

    if use_tls:
        try:
            context = ssl.create_default_context()
            secure_sock = context.wrap_socket(sock, server_hostname=dest_host)
        except ssl.SSLError as e:
            print(f"Error creating SSL/TLS socket: {e}")
            sock.close()
            exit(1)
            
        try:
            # Connect to the server
            secure_sock.connect((dest_host, dest_port))
            net_connect = secure_sock
        except socket.error as e:
            print(f"Error connecting to {dest_host}:{dest_port}: {e}")
            secure_sock.close()
            exit(1)

    else:
        sock.connect((dest_host, dest_port))
        net_connect = sock

    start_time=time.time()
    
    try:
        process = Popen(command, shell=True, stdout=PIPE, stderr=STDOUT, text=True)
        with net_connect, process.stdout:
            for line in iter(process.stdout.readline, "\n"):
                net_connect.sendall(line.encode('utf-8'))
    
    except Exception as e:
        print(f"Error running process: {str(e)}")

    # net_connect.close()
    logging.info("Finished in %s seconds: %s ",time.time()-start_time,command)


def build_cmd_list(buckets,args):
    cli_commands=[]
    for bucket in buckets:
        exporttool_cmd=f"/opt/splunk/bin/splunk cmd exporttool {bucket} /dev/stdout -csv "
        if args.keyval:
            for pair in args.keyval:
                result = re.search(r"..(.*)=(.*)..", str(pair))
                kv="\{result.group(1)}::{result.group(2)}\\"
                exporttool_cmd+="|sed -e 's/^\([[:digit:]]\{10\},\)\(.*\)/\\1"+kv+",\\2/'"
        if args.bucket_name:
            b="\"bucket::"+str(bucket.split("/")[-1:][0])+"\""  #grab the bucket_name from the end of the filepath
            exporttool_cmd+="|sed -e 's/^\([[:digit:]]\{10\},\)\(.*\)/\\1"+b+",\\2/'"  #stick bucket::<bucket_name> right after the time
        cli_commands.append(exporttool_cmd)
        logging.info("exporttool_cmd: %s ",exporttool_cmd)
    return cli_commands

def get_logger(name):
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(name, 'a')
    logger.addHandler(handler)
    return logger

def main():
    args = get_args()
    global logging
    global dest_host
    global dest_port
    global use_tls
    dest_host = args.dest_host
    dest_port = args.dest_port
    use_tls = args.tls
    logging=get_logger(args.logfile)
    logging.info("------------\nStart time: "+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logging.info('Starting a new export using %i streams', args.num_streams)
    logging.info('Beginning Script with these args: %s',' '.join(f'{k}={v}' for k, v in vars(args).items()))
    start_time=time.time()
    buckets=(list_full_paths(args.directory,args.earliest,args.latest,args.import_buckets))
    if args.earliest < args.latest:
        logging.info('Search Min epoch = %i and Max epoch = %i',args.earliest,args.latest)
    else:
        logging.error('ERROR:  The specified Min epoch time (%i) must be less than the specified Max epoch time(%i)',args.earliest,args.latest)
        exit(1)
    logging.info('There are %s buckets in this directory that match the search criteria',len(buckets))
    logging.info('Exporting these buckets: %s',buckets)
    cli_commands=build_cmd_list(buckets, args)
    for cmd in cli_commands:
        with Pool(args.num_streams) as pyool:
            pyool.map(run_cmd_send_data, cli_commands)
    logging.info('Done with script in %s seconds',time.time()-start_time)

if __name__ == "__main__":
    main()