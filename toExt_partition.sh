#!/usr/bin/bash
filename="$1"

CONNECTION="jdbc:hive2://ip-172-31-60-25.ec2.internal:10000/default;principal=hive/ip-172-31-60-25.ec2.internal@EXAMPLE.COM"
BEELINE="beeline -u $CONNECTION --silent=true --outputformat=tsv2 --showHeader=false"
TENANT_SPACE="hdfs:///user/cmoon/tenantspace/ida/anp"

echo "Database,Table,Patition,Location"
while read -r line
do
    DB=$(echo $line | awk -F',' '{print $1}')
    TBL=$(echo $line | awk -F',' '{print $2}')
    PART=$(echo $line | awk -F',' '{print $3}')
    LOC=$(echo $line | awk -F',' '{print $4}')
  

    PART_KEY=`echo $PART | awk -F'=' '{print $1}'`
    PART_VAL=`echo $PART | awk -F'=' '{print $2}'`

    echo "$DB,$TBL,$PART,$LOC"
    echo $NEW_PART_LOC

    PART_DIR=`echo $PART | awk -F'/' '{print $NF}'`
    NEW_PART_DIR=$TENANT_SPACE/$DB/data/$TBL
    NEW_PART_LOC=$NEW_PART_DIR/$PART_DIR

    ($BEELINE -e "ALTER TABLE $DB.$TBL SET TBLPROPERTIES('EXTERNAL'='TRUE');    \
                  ALTER TABLE $DB.$TBL PARTITION($PART_KEY='$PART_VAL') SET LOCATION '$NEW_PART_LOC';")
    hadoop fs -mkdir -p $NEW_PART_DIR
    hadoop fs -mv $LOC $NEW_PART_DIR
done < "$filename"
