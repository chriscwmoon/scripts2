1. toExt_partition.sh
- Input file
- DB, Table, Partition, Location

2. toExt.sh :
- toExt.sh -d <DB> -t <TENANT> -l <LOCATION> [TABLENAME]
- Sample output
```
[ec2-user@ip-172-31-57-245 ~]$ ./toExt.sh -d default -t chrismoon -y t1 

Please find more details on /tmp/toExt_20170712004249.log

==== Input Parameters ====
Database: default
Table: t1
Tenant: chrismoon
New location: hdfs://ip-172-31-54-133.ec2.internal:8020/user/cmoon/chrismoon/data/default/t1
==========================

Testing Connection ... SUCCESS!

Pulling current table properties before making any change ...
Table Type: EXTERNAL_TABLE      
Location: hdfs://ip-172-31-54-133.ec2.internal:8020/user/hive/warehouse/t1


ALTER TABLE t1 SET TBLPROPERTIES('EXTERNAL'='TRUE');

ALTER TABLE t1 SET LOCATION 'hdfs://ip-172-31-54-133.ec2.internal:8020/user/cmoon/chrismoon/data/default/t1'

Creating a HDFS directory for table at ... hdfs://ip-172-31-54-133.ec2.internal:8020/user/cmoon/chrismoon/data/default
This can fail if already exists

Partitions:
 flag=n
flag=y

New Location for partiton : flag=n
hdfs://ip-172-31-54-133.ec2.internal:8020/user/cmoon/chrismoon/data/default/t1/flag=n
New Location for partiton : flag=y
hdfs://ip-172-31-54-133.ec2.internal:8020/user/cmoon/chrismoon/data/default/t1/flag=y

Moving partition directories on HDFS ... 

Please find more details on /tmp/toExt_20170712004249.log
[ec2-user@ip-172-31-57-245 ~]$
```
