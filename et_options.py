# The et_options dictionary (below) contains default configuration settings. Python dictionary format must
# be maintained for this to work properly.

et_options = {
    'directory': '/opt/splunk/etc/apps/botsv3_data_set/var/lib/splunk/botsv3/',  # required if sending to server
    'dest_host': 'route-host',                            # required if sending to server
    'dest_port': 10065,                                   # required if sending to server
    'tls': False,                                         # set to True for TLS (SSL) connection
    'import_buckets': [],                                 # add full paths to buckets for extraction,  as a list
    'num_streams': 2,                                     # set the number of processes to run at a time (higher is not necessarily better)
    'logfile': '/tmp/splunk_to_tcp.log',                  # Location to write/append the logging
    'earliest': 0,                                        # Earliest epoch time for bucket selection
    'latest': 9999999999,                                 # Latest epoch time for bucket selection
    'keyval': [],                                         # Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw.
    'bucket_name': False,                                 # add bucket name to output
    'file_out': False,                                    # if set to True, network output is disabled
    'file_out_path': '/data/logs/cribl/',                 # destination folder if file_out is set to True
    'check_hostname': False                               # set to False to disable checking of certificate hostname during SSL connection
}
