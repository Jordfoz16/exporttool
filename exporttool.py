#!/usr/bin/python3

# Written by Jim Apger, Cribl
#
# This is to be run on a Splunk Indexer for the purpose of exporting buckets and streaming their contents to Cribl Stream
#
# Features:
#   traditional, Smart Store, and frozen buckets
#   skips cluster replicated and hot buckets
#
# *** Configuration options are stored in et_options.py
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
# Version 2.1.0 (November 2023) - Brant ﬁ
#   - added native libraries to eliminate the requirement for netcat
#   - added the ability to specify options in a configuration file
# Version 2.1.1 (November 2023) - Brant ∏
#   - replace tomllib requirement for configuration file
#   - added option for output to csv (text) file - uses Splunk exporttool defaults
# Version 2.1.2 (December 2023) - Brant ∆
#   - convert output format to JSON
#   - removed dependency on the existence of the "punct" field for determining record boundary
# Version 2.1.3 (December 2023) - Brant ◊
#   - removed cli options, control using et_options.py config
# Version 2.1.4 (December 2023) - Brant Ψ
#   - added option to write data to file, in the same format that is sent over the network
# Version 2.1.5 (January 2024) - Brant »
#   - removed extraneous code
#   - formatted and commented code for readability
# Version 2.1.6 (January 2024) - Brant
#   - option "file_out_compress" for writing json records directly to gzip compressed file
#   - added some stdout output to see process progress (also in log file)

import glob
from subprocess import Popen, PIPE, STDOUT
import time
import logging
import re
import datetime
import socket
import ssl
import json
import gzip
import os
from multiprocessing import Pool

# read job configuration file (et_options.py)
try:
    from et_options import et_options
except ImportError:
    print("Configuration values are kept in et_options.py, please add them there!")
    raise

def record_format(cur_rec):
    # split out primary metadata fields
    record_split = re.search(
        r"^(\d+)..source::(.*?)\",\"host::(.*?)\",\"sourcetype::(.*?)\",\"(.*?)\",\"_indextime::",
        cur_rec,
        re.DOTALL | re.MULTILINE,
    )
    dict_record = {}
    try:
        # populate dictionary with raw and other primary metadata fields, for JSON output
        dict_record["time"] = record_split.group(1)
        dict_record["source"] = record_split.group(2)
        dict_record["host"] = record_split.group(3)
        dict_record["sourcetype"] = record_split.group(4)
        dict_record["raw"] = record_split.group(5)
        dict_record["index"] = index_name(et_options["directory"])
        json_record = json.dumps(dict_record)
    except Exception as e:
        print(f"result is None: {str(e)}")
    return json_record + "\n"

def index_name(path):
    # Split the path into parts and filter out any empty strings
    path_parts = [part for part in path.split(os.sep) if part]
    # Check if there are at least two parts in the path
    if len(path_parts) < 2:
        return None  # Return None if there aren't enough directories
    # Return the second-to-last part
    return path_parts[-2]

def list_full_paths(directory, earliest, latest, import_buckets):
    # processes all buckets listed under the "import_buckets" option in et_options.py if specified
    # if "import_buckets" not specified, processes all buckets under the path given in the "directory" option
    files = []
    buckets = []
    if len(import_buckets) > 0:
        for line in import_buckets:
            files.append(line.strip())
    else:
        files_to_process = glob.glob(f"{directory}/**/*.tsidx", recursive=True)
        files = files_to_process
    for file in files:
        tsidx = file.split("/")[-1]  # the tsidx filename less the path
        max_epoch = tsidx.split("-")[-3]  # Grab max and min from file name
        min_epoch = tsidx.split("-")[-2]  # Grab max and min from file name
        bucketName = file.split("/")[
            -2
        ]  # Grab the name of the bucket (parent dir for the tsidx filename)
        if (
            earliest <= int(max_epoch)
            and latest >= int(min_epoch)
            and "DISABLED" not in file
            and not bucketName.startswith("rb_")
            and not bucketName.startswith("hot")
        ):  # filter buckets if user passed min/max epoch times
            # Will assume everything is a bucket except for dir names that contain DISABLED, are cluster associated replicated buckets (tested for non-smartstore), or hot buckets
            # For an on-prem config, we might find multiple tsidx files in an index.  Only grab the iunique parent directory containing these tsidx files once.
            buckets.append(
                re.sub("\/[^\/]+$", "", file)
            )  # strip the .tsidx filename from the path to only include the parent dir
    return set(buckets)


def run_cmd_send_data(command):
    # determine type of output, network or file (and what file type)
    start_time = time.time()
    if file_out:
        if file_out_type == "seo":
            file_out_name = file_out_path + command.split()[3].split("/")[-1] + ".csv"
            file_out_command = command.split()
            file_out_command[4] = file_out_name
            file_out_command = " ".join(file_out_command)
            seo_file_output(file_out_command)
        elif file_out_type == "exo":
            file_out_name = file_out_path + command.split()[3].split("/")[-1] + ".json"
            file_out_command = command.split()
            file_out_command = " ".join(file_out_command)
            exo_file_output(file_out_command, file_out_name)
    else:
        net_output(command)
    logging.info(
        f"{time.time()-start_time:7.2f} seconds to process: {command.split()[3]}"
    )
    print(f"{time.time()-start_time:7.2f} seconds to process: {command.split()[3]}")


def seo_file_output(command):
    # run with (seo) exporttool default output (csv), with relative filename
    try:
        process = Popen(
            command,
            shell=True,
            stdout=PIPE,
            stderr=STDOUT,
            universal_newlines=True,
            encoding="utf-8",
        )
        process.wait()
    except Exception as e:
        print(f"Error running process: {str(e)}")


def exo_file_output(command, file_out_name, compressionlevel=5):
    try:
        if file_out_compress:
            file_out_name = file_out_name + ".gz"
            open_args = {
                "filename": file_out_name,
                "mode": "wt",
                "compresslevel": compressionlevel,
            }
            open_func = gzip.open
        else:
            open_args = {"filename": file_out_name, "mode": "w"}
            open_func = open

        with open_func(**open_args) as exo_file:
            try:
                process = Popen(
                    command,
                    shell=True,
                    stdout=PIPE,
                    stderr=STDOUT,
                    universal_newlines=True,
                    encoding="utf-8",
                )
                current_rec = ""
                for line in process.stdout:
                    if not line:
                        break
                    if header in line or "log-cmdline.cfg" in line:
                        continue
                    if line[0:10].isdigit():
                        if current_rec:
                            exo_file.write(record_format(current_rec))
                        current_rec = line
                    else:
                        current_rec += line
                if current_rec:
                    exo_file.write(record_format(current_rec))
            except Exception as e:
                print(f"Error running process: {str(e)}")
    except Exception as e:
        print(f"Error opening file {str(e)}")


def net_output(command):
    # net_output encodes data and sends it over socket, with TLS enabled, if specified.
    # this function also assembles full records for the output
    sock = None
    secure_sock = None
    process = None

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if use_tls:
            context = ssl.create_default_context()
            context.verify_mode = ssl.CERT_NONE
            context.check_hostname = False
            secure_sock = context.wrap_socket(sock, server_hostname=dest_host)
            secure_sock.connect((dest_host, dest_port))
            net_connect = secure_sock
        else:
            sock.connect((dest_host, dest_port))
            net_connect = sock
        process = Popen(
            command,
            shell=True,
            stdout=PIPE,
            stderr=STDOUT,
            universal_newlines=True,
            encoding="utf-8",
        )
        current_rec = ""
        for line in process.stdout:
            if not line:
                break
            if header in line or "log-cmdline.cfg" in line:
                continue
            if line[0:10].isdigit():
                if current_rec:
                    net_connect.send(record_format(current_rec).encode("utf-8"))
                current_rec = line
            else:
                current_rec += line
        if current_rec:
            net_connect.send(record_format(current_rec).encode("utf-8"))
    except socket.error as e:
        print(f"Error sending data over the socket: {e}")
    except Exception as e:
        print(f"Error running process: {str(e)}")
    finally:
        if process:
            process.stdout.close()
        if secure_sock:
            secure_sock.close()
        elif sock:
            sock.close()


def build_cmd_list(buckets):
    # builds string of commands that is worked through by the number of processes specified
    # as "num_streams" in the configuration file
    cli_commands = []
    for bucket in buckets:
        exporttool_cmd = (
            f"{splunk_home}/bin/splunk cmd exporttool {bucket} /dev/stdout -csv "
        )
        cli_commands.append(exporttool_cmd)
        logging.info(f"exporttool_cmd: {exporttool_cmd}")
    return cli_commands


def get_logger(name):
    # define and initialize file that the run log is written to
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    try:
        handler = logging.FileHandler(name, "a")
    except Exception as e:
        print(f"Error accessing log file: {str(e)}")
    logger.addHandler(handler)
    return logger


def main():
    

    # initialize config file option variables as global variables
    global header, logging, dest_host, dest_port, use_tls, num_streams, file_out, file_out_path, file_out_type, splunk_home, file_out_compress
    header = '"_time",source,host,sourcetype,"_raw","_meta"'
    splunk_home = et_options["splunk_home"]
    dest_host = et_options["dest_host"]
    dest_port = et_options["dest_port"]
    use_tls = et_options["tls"]
    num_streams = et_options["num_streams"]
    file_out = et_options["file_out"]
    file_out_path = et_options["file_out_path"]
    file_out_type = et_options["file_out_type"]
    file_out_compress = et_options["file_out_compress"]

    # write run header to logfile
    logging = get_logger(et_options["logfile"])
    logging.info("-" * 25)
    logging.info(f"Start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Starting a new export using {num_streams} streams")
    logging.info(f"Beginning Script with these et_options: {et_options}")
    start_time = time.time()
    buckets = list_full_paths(
        et_options["directory"],
        et_options["earliest"],
        et_options["latest"],
        et_options["import_buckets"],
    )

    # sanity check timing scope configuration specified
    if et_options["earliest"] < et_options["latest"]:
        logging.info(
            f"Search Min epoch = {et_options['earliest']} and Max epoch = {et_options['latest']}"
        )
    else:
        logging.error(
            f"ERROR: The specified Min epoch time ({et_options['earliest']}) must be less than the specified Max epoch time({et_options['latest']})"
        )
        exit(1)

    # add job information to the log file
    logging.info(
        f"There are {len(buckets)} buckets in this directory that match the search criteria"
    )
    logging.info(f"Exporting the following buckets:")
    for b in buckets:
        logging.info(b)
    cli_commands = build_cmd_list(buckets)
    print("-" * 25)
    print(f"processing {len(cli_commands)} index files")
    print("-" * 25)
    for proc in cli_commands:
        print(proc.split()[3])
    print("-" * 25)
    print("Chosen Configuration:")
    if file_out:
        print(f"File Output! <---")
        print(f"Output Path = {file_out_path}")
        print(f"File Type = {file_out_type}")
        print(f"Compressed = {file_out_compress}")
    else:
        print(f"Network Output! <---")
        print(f"Destination = {dest_host}:{dest_port}")
        print(f"TLS = {use_tls}")
    print(f"Max Concurrent Processes = {num_streams}")
    print("-" * 25)
    print()
    continue_prompt = input(
        "Getting ready to process the above list, with displayed configuration. Continue? (y/n): "
    )
    if continue_prompt.lower() == "y":
        # create processes from command list, run as many processes as specified until list exhauseted
        with Pool(num_streams) as pyool:
            pyool.map(run_cmd_send_data, cli_commands)
        proc_time = time.time() - start_time
        logging.info(
            f"Completed processing in {str(datetime.timedelta(seconds=proc_time))}"
        )
        print(f"Completed processing in {str(datetime.timedelta(seconds=proc_time))}")
    else:
        quit()


if __name__ == "__main__":
    main()
