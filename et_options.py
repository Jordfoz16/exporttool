# The et_options dictionary (below) contains default configuration settings. Python dictionary format must
# be maintained for this to work properly.

et_options = {
    "splunk_home": "/opt/splunk/",
    "directory": "/opt/splunk/var/lib/splunk/suricata/",
    "dest_host": "x-tractor",
    "dest_port": 10065,
    "tls": False,
    "check_hostname": False,
    "import_buckets": [],
    "num_streams": 2,
    "logfile": "/tmp/splunk_to_tcp.log",
    "earliest": 0,
    "latest": 9999999999,
    "bucket_name": False,
    "only_db": False,
    "file_out": False,
    "file_out_path": "/data/logs/cribl/",
    "file_out_type": "exo",
    "file_out_compress": True,
}

# splunk_home       path of the splunk installation
# directory:        required if sending to server
# dest_host:        required if sending to server
# dest_port:        required if sending to server
# tls:              set to True for TLS (SSL) connection
# check_hostname:   set to False to disable checking of certificate hostname during SSL connection
# import_buckets:   add full paths to buckets for extraction, as a list
# num_streams:      set the number of processes to run at a time (higher is not necessarily better)
# logfile:          Location to write/append the logging
# earliest:         Earliest epoch time for bucket selection
# latest:           Latest epoch time for bucket selection
# bucket_name:      add bucket name to output
# only_db:          only read files that start with db_ (True or False)
# file_out:         if set to True, network output is disabled and results are sent to disk instead.
# file_out_path:    destination folder if file_out is set to True
# file_out_type:    'seo' for default Splunk exporttool (csv) out. 'exo' for JSON formatted output
# file_out_compress:    only applicable when writing to file with file_out_type='exo'
