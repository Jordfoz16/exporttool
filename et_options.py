et_options = {
    'directory': '/opt/splunk/var/lib/splunk/suricata/',  # required if sending to server
    'dest_host': 'route-host',                            # required if sending to server
    'dest_port': 10065,                                   # required if sending to server
    'tls': False,                                         
    'import_buckets': [],                                 # add full paths to buckets for extraction,  as a list
    'num_streams': 2,
    'logfile': '/tmp/splunk_to_tcp.log',
    'earliest': 0,
    'latest': 9999999999,
    'keyval': [],
    'bucket_name': False,
    'file_out': True,
    'file_out_path': '/data/logs/cribl/',
    'gzip_file': False
}
