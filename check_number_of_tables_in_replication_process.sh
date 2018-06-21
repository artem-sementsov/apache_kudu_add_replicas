#!/bin/bash
config_file=add_replicas_all_tables.config
#config_file=$1

saveIFS="$IFS"

nl=$'\n'

unset prefix

if [ -z $config_file ]
then
   echo "Some input variables are missed!"
   echo "Usage : add_replicas_all_tables.sh [Path to config]"
   exit 0     
else 

sed 's/\r//g' -i $config_file

IFS="="
while read var value || [ -n "$value" ];
do
    export "$var"="$value"
done < $config_file

IFS=' '
tablet_server_list=($str_tablet_server_list)
tablet_server_uuid=($str_tablet_server_uuid)

ts_cnt=${#tablet_server_list[@]}

IFS=$'\n'
echo "Tablet_server_list : ${tablet_server_list[@]}"
echo "Tablet_server_uuid : ${tablet_server_uuid[@]}"
echo "Kudu_masters : $kudu_masters"
echo "Number_of_replicas : $number_of_replicas"

info=()
tablet_list=()
table_name_list=()
temp_table_name_list=()
releation_tablet=()
i=0
while [ $i -lt ${#tablet_server_list[@]} ]
do
	temp=($(kudu remote_replica list ${tablet_server_list[$i]}))
	echo "TS ${tablet_server_list[$i]} : ${#temp[@]}"
	info+=(${temp[@]})
	tablet_list+=($(echo "${temp[@]/%/$'\n'}" | grep "Tablet id:" | awk 'BEGIN{FS=": "} {print $2}'))
	temp_table_name_list=($(echo "${temp[@]/%/$'\n'}" | grep "Table name:" | awk 'BEGIN{FS=": "} {print $2}'))
	table_name_list+=(${temp_table_name_list[@]})
	releation_tablet+=($i)
	j=1
	while [ $j -lt ${#temp_table_name_list[@]} ]
	do
		releation_tablet+=($i)
		j=$[$j+1]
	done
	i=$[$i+1]
done

echo "All info: ${#info[@]}"
echo "Row count about tablet (tablet count): ${#tablet_list[@]}"
echo "Row count about table: ${#table_name_list[@]}"
echo "Row count about TS: ${#releation_tablet[@]}"

echo "Calculating map of tablet's replicas count..."
unset table_map
declare -A table_map
i=0
while [ $i -lt ${#tablet_list[@]} ]
do 
	tablet_id=${tablet_list[$i]}
	table_name=${table_name_list[$i]}
	table_name="${table_name/importdb./}"
	table_name="${table_name/migration./}"
	if [ -n $prefix ]; then
		if [[ $table_name == $prefix* ]]; then
			((++table_map[$tablet_id]))
		fi
	else 
		((++table_map[$tablet_id]))
	fi
	i=$[$i+1]
done

echo "Number of tablets: ${#table_map[@]}"

unset table_map_2
declare -A table_map_2
for i in ${!table_map[@]};
do 
	if [[ ${table_map[$i]} -lt $number_of_replicas ]]; then
		table_map_2[$i]=${table_map[$i]}
	fi
done

echo "Number of tablets with less than $number_of_replicas replicas: ${#table_map_2[@]}"

# for i in ${!table_map_2[@]};
# do 
	# echo $i ${table_map_2[$i]}
# done

# IFS=$'\n'
# echo "${info[@]/%/$'\n'}" > log.txt  | grep "ffb2bbadff8b4f84a7135cc4e1be558c"

fi
