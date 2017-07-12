#!/bin/bash


HIVESERVER_1=
HIVESERVER_2=

BEELINE="beeline -u 'jdbc:hive2://hiveserver.c21.hadoop.td.com:10000/default;principal=hive/hiveserver.c21.hadoop.td.com@C21.HADOOP.TD.COM'"

scp $1 $HIVESERVER_1:/lib/aux/lib
scp $1 $HIVESERVER_2:/lib/aux/lib
  
$(BEELINE -e "reload")
$(BEELINE -e "reload") 
 
pid1=`ssh $HIVESERVER_1 "ps -ef | grep -e HiveServer2  | grep -v grep | awk {'print $2'}"`
pid2=`ssh $HIVESERVER_2 "ps -ef | grep -e HiveServer2  | grep -v grep | awk {'print $2'}"`

whie [ `ssh $HIVESERVER_1 "sudo lsof -p $pid1 | grep $1 | wc -l` != 0 ] 
do
  $(BEELINE -e "reload")
done

whie [ `ssh $HIVESERVER_2 "sudo lsof -p $pid2 | grep $1 | wc -l` != 0 ] 
do
  $(BEELINE -e "reload")
done

