import sys

import yaml

def yaml_xtract(arg_str):
    if not arg_str:
        return {}

    if arg_str.startswith('.') or arg_str.startswith('/'):
        with open(arg_str, 'rb') as f:
            return yaml.load(f.read())

    return yaml.load(arg_str)

def shout(msg, f=sys.stdout):
    f.write(msg)
    f.flush()

def escape_s3uri(s3_uri):
    s3_uri = s3_uri + '/' if not s3_uri.endswith('/') else s3_uri
    bk_name, key_name = re.match('^s3://([^/]+)/(.*)$', s3_uri).groups()
    key_name = '' if key_name is None else key_name
    return bk_name, key_name

