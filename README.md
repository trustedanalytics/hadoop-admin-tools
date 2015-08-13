# hadoop-admin-tools
Admin tool kit helpful in configuration cloud foundry brokers for hadoop services.

## Build 
Run command for compile and package.: 
```
mvn clean package
```
## Avaliable tools

### import_hadoop_conf.sh
Download, unzip, convert to JSON the contents of a zip archive with hadoop client configuration. 
Prepared JSON print on the standard output.

Examples:

Getting hadoop client configuration directly from CDH manager.
```
./import_hadoop_conf.sh -cu http://<cloudera_manager_host_name>:7180/cmf/services/3/client-config
```

Getting hadoop client configuration from local archive.
```
./import_hadoop_conf.sh -cu file://path/client-config.zip
```

Getting hadoop client configuration from stdin.
```
cat /path/client-config.zip | ./import_hadoop_conf.sh
```

Usage:
```
./import_hadoop_conf.sh -h
Usage: <main class> [options]
  Options:
    -help, --help, -h, ?
       display usage help
       Default: false
    -configUrl, -cu
       Location of configuration hadoop client archive (zip). It can be local
       (file://path), or remote (http(s)://host/path).
    -v, -verbose
       
       Default: false
```
