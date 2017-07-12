#!/bin/bash


# Please Fill out following connection 
CONNECTION="jdbc:hive2://ip-172-31-60-25.ec2.internal:10000/default;principal=hive/ip-172-31-60-25.ec2.internal@EXAMPLE.COM"
#TENANT_SPACE="hdfs://nameservice1/tenantspace/ida/anp"
TENANT_SPACE="hdfs://ip-172-31-54-133.ec2.internal:8020/user/cmoon"

# You can leave it blank for the arguments below
DATABASE=
TABLE=
LOCATION=
BEELINE="beeline -u $CONNECTION --silent=true --outputformat=tsv2 --showHeader=false"
YES=true
LOG="/tmp/toExt_`date \"+%Y%m%d%H%M%S\"`.log"

echo
echo "Please find more details on $LOG"
echo 

while getopts "d:l:t:y" opt; do
  case $opt in
    d)
      DATABASE=$OPTARG
      ;;
    l)
      LOCATION=$OPTARG
      ;;
    t)
      TENANT=$OPTARG
      ;;
    y)
      YES=false ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      echo "Usage: toExternal.sh -d <DB> -t <Tenant> -l <new_location> <TableName>"
      exit
      ;;
    esac
done
shift "$((OPTIND-1))"

TABLE=$1

########
# validate new location

if [ "$LOCATION" = "" ]
then
    LOCATION="$TENANT_SPACE/$TENANT/data/$DATABASE/$TABLE"
fi

echo "==== Input Parameters ===="
echo "Database: $DATABASE"  | tee $LOG
echo "Table: $TABLE"        | tee $LOG
echo "Tenant: $TENANT"      | tee $LOG
echo "New location: $LOCATION" | tee $LOG
echo "==========================" | tee $LOG
echo 
if ! grep -q $TENANT_SPACE <<< $LOCATION;then 
  echo "ERROR: New location has to be part of tenant space ($TENANT_SPACE)" | tee $LOG
  echo "       Location is set to $LOCATION"   | tee $LOG
  exit
fi

while $YES; do
    read -p "Do you wish to continue?" yn
    case $yn in
        [Yy]* )
           if [ "$DATABASE" = "" -o "$TABLE" = "" -o "$TENANT" = "" ]
           then
               echo "ERROR: One of the fields are empty" 
               exit
           fi
           break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes(y) or no(n).";;
    esac
done

# Test Connection
printf "Testing Connection ... " | tee -a $LOG
DBS=`$BEELINE -e "show databases" 2>> $LOG`
### Also, consider checking kerberos
if [ "$DBS" = "" ]
then
    echo "FAIL!"
    echo "CONNECTION ERROR"
    exit 1
else
  echo "SUCCESS!"
fi

###############################
# Current status of the table before change
#
echo
echo "Pulling current table properties before making any change ..."
CMD="DESCRIBE FORMATTED $DATABASE.$TABLE"
CUR_LOC=`$BEELINE -e "$CMD" | grep 'Location' | awk -F $'\t' '{print $2}'` 
CUR_TYPE=`$BEELINE -e "$CMD" | grep 'Table Type' | awk -F $'\t' '{print $2}'`

echo "Table Type: $CUR_TYPE"
echo "Location: $CUR_LOC"
echo
###############################

while $YES; do
    read -p "Do you want to continue??" yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes(y) or no(n).";;
    esac
done
echo
#######
# Convert to external
CMD="ALTER TABLE t1 SET TBLPROPERTIES('EXTERNAL'='TRUE');"
echo "$CMD" | tee -a $LOG
($BEELINE -e "$CMD")
echo

#########
# Convert Table Location
CMD="ALTER TABLE $TABLE SET LOCATION '$LOCATION'"
echo "$CMD" | tee -a $LOG
($BEELINE -e "$CMD")
echo 

#######
# mkdir : new table location
NEW_LOC_PARENT=`echo $LOCATION | awk -F'/' '{OFS=FS;$(NF--);print}'`
echo "Creating a HDFS directory for table at ... $NEW_LOC_PARENT"
hadoop fs -mkdir -p $NEW_LOC_PARENT
echo

########
# Get Partitions
CMD="show partitions $1"
PARTITIONS=`$BEELINE -e "$CMD"`

echo "Partitions:"
echo " $PARTITIONS"
echo
#########
# Convert Partitions Location
for part in $PARTITIONS 
do
    #### assuming partition name is "year=2017"

    # part_key : year
    PART_KEY=`echo $part | awk -F'=' '{print $1}'`
    
    # part_val : 2017
    PART_VAL=`echo $part | awk -F'=' '{print $2}'`
    
    CMD="DESCRIBE FORMATTED default.t1 PARTITION ($PART_KEY='$PART_VAL')"
    
    # current partition location
    CUR_PART_LOC=`$BEELINE -e "$CMD" | grep 'Location' | awk -F $'\t' '{print $2}'`
    
    # directory name of the partition( eg. year=2017 )
    PART_DIR=`echo $CUR_PART_LOC | awk -F'/' '{print $NF}'`
    
    # new partition location
    NEW_PART_LOC=$LOCATION/$PART_DIR
    
    echo "New Location for partiton : $part" | tee -a $LOG
    echo "$NEW_PART_LOC" | tee -a $LOG

   ($BEELINE -e "ALTER TABLE $DATABASE.$TABLE PARTITION($PART_KEY='$PART_VAL') SET LOCATION '$NEW_PART_LOC'")
    
   # echo "Moving partition directory on HDFS ... " | tee -a $LOG
   # hadoop fs -mv $PART_LOC $LOCATION
done

echo
NEW_LOC_PARENT=`echo $LOCATION | awk -F'/' '{OFS=FS;$(NF--);print}'`
echo "Moving partition directories on HDFS ... " | tee -a $LOG
echo "Moving from $CUR_LOC to $NEW_LOC_PARENT" >> $LOG
hadoop fs -mv $CUR_LOC $NEW_LOC_PARENT
echo
echo "Please find more details on $LOG"
