# cdp_migration_assist
Assist HDP to CDP migration

Fill in the ambari connection properties in the config.ini and launch the script as below:

```
bash-3.2$ python3 ambari_discovery.py 
DEBUG - MainThread - ambari_discovery - 2022-03-14 00:57:31,141 - Current timestamp to use in metric collection: 2022-03-14T04:57:31
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:31,142 - Read host list.
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:31,486 - Api response stored in: ./output/read_hosts.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:31,486 - Read host information.
ERROR - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:32,218 - Requested URL not found. Error:HTTP Error 404: Not Found
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,438 - Api response stored in: ./output/hosts_memory.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,439 - Api response stored in: ./output/hosts_vcores.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,439 - Api response stored in: ./output/hosts_physical_cpu.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,439 - Api response stored in: ./output/hosts_operating_system.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,439 - Api response stored in: ./output/hosts_hdfs_disk_space.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,439 - Api response stored in: ./output/hosts_rack_info.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:37,440 - Read service information.
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:39,298 - Api response stored in: ./output/yarn_info.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:39,298 - Api response stored in: ./output/capacity_scheduler.json
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:39,299 - Read service information.
DEBUG - ambari_api_thread - ambari_cluster_extractor - 2022-03-14 00:57:39,645 - Api response stored in: ./output/kerberos_info.json
```
