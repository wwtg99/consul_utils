Consul Utils
============


# Introduction

[Consul](https://www.consul.io/) is a highly available and distributed service discovery and KV store designed with support for the modern data center. Consul provides a useful UI but it is not convenient for automation and batch processing. Consul-utils is a useful command line tool for these jobs based on Consul's API.

# Installation

Installation is easy by pip
```
pip install consul_utils
```

# Usage

Show help by
```
consul_utils --help
```

Consul-utils provides several useful tools to handle Consul.

## Configuration

First you should create a configuration file to save common settings like below:

```
# consul configuration
consul:
  # consul host
  host: "test.consul.com"
  # consul port
  port: 8500
  # consul scheme
  scheme: "http"
  # consul ACL token
  token: ""
  # default root
  root: ""
# cache configuration
cache:
  # cache enabled or not
  cache_enabled: true
  # cache file
  cache_dir: ".consul_cache"
  # cache expire seconds
  cache_ttl: 600
# log configuration
log:
  # log level
  log_level: "INFO"
# output configuration
reporter:
  # result output type, text, json or csv
  output_type: "text"
  # result output file, leave empty to print to console
  output_file: ""
  # output all scan data
  show_all_scan: false
  # output filtered data
  show_filtered: true
  # output not filtered data
  show_no_filtered: false
  # output flags data
  show_flags: false
# search command configuration
search:
  # search results limit
  limit: 10
  # search fields, keys or values
  fields: "keys"
  # use regex for search or not
  regex: false
```

Save this file to `config.yml`, remember it is not required and all settings can be specified by command line option. If same settings exists both in config file and options, the options value will override config file.

## Dump Consul key values

Dump key and values

```
consul_utils dump -c config.yml
```

Specify directory

```
consul_utils dump -c config.yml -r test/test_root
```

Change output type, text (default), json or csv

```
consul_utils dump -c config.yml -r test/test_root -x json
```

Output to file instead of console

```
consul_utils dump -c config.yml -r test/test_root -o out.txt
```

## Search in the Consul key values

Search keys that contains `test`
```
consul_utils search -c config.yml -q test
```

Search values that contains `test`

```
consul_utils search -c config.yml -q test -f values
```

Limit output result number, default 10

```
consul_utils search -c config.yml -q test --limit 5
```

Use regex to search

```
consul_utils search -c config.yml -q ^test$ -e
```

## Copy key values from one place to another

Copy key values under source root to target root

```
consul_utils copy -c config.yml --root test/source --target-root test/target
```

## Compare two key values

Compare two key values and all sub key values under two specified root
This command will compare all values for keys with the same relative path.

```
consul_utils diff -c config.yml --root1 test1/aa --root2 test2/bb
```

Compare key values from two different host. If not specified, will use the default (host, port, scheme, token) settings.

```
consul_utils diff -c config.yml --host1 test1.consul.com --root1 test1/aa --host2 test2.consul.com --root2 test2/bb
```

# Tests

Prepare a consul node at http://test.consul.com:8500 (you can change hosts file).

install pytest and run by pytest

```
pytest
```

# Authors

Wu Wentao
