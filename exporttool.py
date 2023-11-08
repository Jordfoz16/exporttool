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
#  -d  Source directory pointing at the index.
#  -r  Remote address to send the exported data to.  This should be your Cribl worker with a TCP listener opened up.
#  -p  Remote TCP port to be used
# Optional:
#  -t  Send with TLS enabled
#  -n  Number of parallel stream to utilize
#  -l  Location to write/append the logging
#  -et Earliest epoch time for bucket selection
#  -lt Latest epoch time for bucket selection
#  -kv Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.  Can specify -kv multiple times.
#  -b  Add the bucket=<bucketname> kv pair to the output
#  -i  Import buckets from a file rather than crawl the provided directory
#
# Make sure nc (netcat) is in the path or hard code it below to fit your needs
#
# If you use the -kv option, make sure your pipeline in Cribl Stream accounts for the new field(s)
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
#   - added asyncio and SSL functionality to eliminate the requirement for netcat
#   - added the ability to specify options in a configuration file (can be overridden on CLI)


import argparse
import configparser
import os
import subprocess
import sys
import time
import logging
import re
import datetime
# import io
# import socket
import ssl
import asyncio
from asyncio import StreamWriter
from multiprocessing import Pool

def get_args(argv=None):
    parser = argparse.ArgumentParser(description="This is to be run on a Splunk Indexer for the purpose\
            of exporting buckets and streaming their contents to Cribl Stream")
    parser.add_argument("--tls", help="Send with TLS enabled", action='store_true')
    parser.add_argument("--import_buckets", help="Import buckets from a file (specifid by the -d arg) rather than crawl the provided directory", action='store_true')
    parser.add_argument("--num_streams", default="1", type=int, help="Number of parallel streams to utilize")
    parser.add_argument("--logfile", default="/tmp/SplunkToCribl.log", help="Location to write/append the logging")
    parser.add_argument("--earliest", default=0, type=int, help="Earliest epoch time for bucket selection")
    parser.add_argument("--latest", default=9999999999, type=int, help="Latest epoch time for bucket selection")
    parser.add_argument("--keyval", action='append', nargs='+', help="Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.  Can specify -kv multiple times")
    parser.add_argument("--bucket_name", help="Add bucket=<bucketname> to the output",action='store_true')
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("-d","--directory", help="Source directory pointing at the index", required=True)
    requiredNamed.add_argument("-r","--dest_host", help="Remote address to send the exported data to", required=True)
    requiredNamed.add_argument("-p","--dest_port", help="Remote TCP port to be used", required=True)
    return parser.parse_args(argv)

def list_full_paths(directory,earliest,latest,import_buckets):
    #  We are accounting for directory structures specific to traditional on-prem and Smart Store
    #  Use the -i arg is you are mounting a SmartStore directory (speed) or importing individual buckets
    #  The one thing common to them that contains min/maw epoch times is the .tsixd file
    #  Smart Store dir example:  _internal/db/bd/e3/14~676B2388-3181-4A73-BD1E-43F02EF050B4/guidSplunk-676B2388-3181-4A73-BD1E-43F02EF050B4/1668952678-1668520680-9018843933635107078.tsidx
    #  Traditional dir example:  _internaldb/db/db_1674422056_1673990057_8/1674309022-1673990057-8288841824203874392.tsidx
    buckets=[]
    files=[]
    if import_buckets:
        bucket_file = open(directory, "r")
        for line in bucket_file:
            if not line.startswith("#"):
                files.append(line.strip())
    else:
        for root, dirs, filesInDir in os.walk(directory, topdown=True):
            for file in filesInDir:
                if "tsidx" in file:
                    files.append(os.path.join(root, file))

    # files[] contains the full paths, including the .tsidx filname
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


async def send_data(dest_host, dest_port, command, use_tls):
    if use_tls:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # ssl_context.load_cert_chain(certfile='your_cert.pem', keyfile='your_key.pem')
    else:
        ssl_context = None

    reader, writer = await asyncio.open_connection(dest_host, dest_port, ssl=ssl_context)

    try:
        async for record_line in run_cmd(command):
            writer.write(record_line)
            await writer.drain()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


def build_cmd_list(buckets,args):
    cli_commands=[]
    for bucket in buckets:
        exporttool_cmd=f"/opt/splunk/bin/splunk cmd exporttool {bucket} /dev/stdout -csv "
        if args.keyval:
            for pair in args.keyval:
                result = re.search(r"..(.*)=(.*)..", str(pair))
                kv="\{result.group(1)}::{result.group(2)}\\"
                exporttool_cmd+="|sed -e 's/^\([[:digit:]]\{10\},\)\(.*\)/\\1"+kv+",\\2/'"
        if args.bucketname:
            b="\"bucket::"+str(bucket.split("/")[-1:][0])+"\""  #grab the bucketname from the end of the filepath
            exporttool_cmd+="|sed -e 's/^\([[:digit:]]\{10\},\)\(.*\)/\\1"+b+",\\2/'"  #stick bucket::<bucketname> right after the time
        # exporttool_cmd+="| nc "
        # if args.TLS:
        #     exporttool_cmd+="--ssl "
        # exporttool_cmd+=args.remoteIP
        # exporttool_cmd+=" "+args.remotePort
        cli_commands.append(exporttool_cmd)
        logging.info("exporttool_cmd: %s ",exporttool_cmd)
    return cli_commands

def run_cmd(cmd):
        start_time=time.time()
        p = subprocess.Popen(cmd,  shell=True, encoding='utf-8',stderr=subprocess.PIPE)
        while True:
            out = p.stderr.read(1)
            if out == '' and p.poll() != None:
                break
            if out != '':
                yield out
                # sys.stdout.write(out)
                # sys.stdout.flush()
        logging.info("Finished in %s seconds: %s ",time.time()-start_time,cmd)

def get_logger(name):
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(name, 'a')
    logger.addHandler(handler)
    return logger

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    arg_vals = None
    args = get_args(arg_vals)
    global logging
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
    cli_commands=build_cmd_list(buckets,args)
    for cmd in cli_commands:
        with Pool(args.num_streams) as p:
            p.map(send_data(dest_host=args.dest_host, dest_port=args.dest_port, command=cmd, use_tls=args.tls))
    logging.info('Done with script in %s seconds',time.time()-start_time)

if __name__ == "__main__":
    main()
