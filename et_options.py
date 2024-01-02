# The et_options dictionary (below) contains default configuration settings. Python dictionary format must
# be maintained for this to work properly.

et_options = {
    'splunk_home': '/opt/splunk/',
    'directory': '/opt/splunk/etc/apps/botsv3_data_set/var/lib/splunk/botsv3/',
    'dest_host': 'route-host',
    'dest_port': 10065,
    'tls': False,
    'check_hostname': False,
    'import_buckets': [],
    'num_streams': 2,
    'logfile': '/tmp/splunk_to_tcp.log',
    'earliest': 0,
    'latest': 9999999999,
    'keyval': [],
    'bucket_name': False,
    'file_out': True,
    'file_out_path': '/data/logs/cribl/',
    'file_out_type': 'exo'
}

# directory:        required if sending to server
# dest_host:        required if sending to server
# dest_port:        required if sending to server
# tls:              set to True for TLS (SSL) connection
# check_hostname:   set to False to disable checking of certificate hostname during SSL connection
# import_buckets:   add full paths to buckets for extraction,  as a list
# num_streams:      set the number of processes to run at a time (higher is not necessarily better)
# logfile:          Location to write/append the logging
# earliest:         Earliest epoch time for bucket selection
# latest:           Latest epoch time for bucket selection
# keyval:           Specify key=value to carry forward as a field in addition to _time, host, source, sourcetype, and _raw
# bucket_name:      add bucket name to output
# file_out:         if set to True, network output is disabled
# file_out_path:    destination folder if file_out is set to True
# file_out_type:    'seo' for default Splunk exporttool (csv) out. 'exo' for JSON formatted output