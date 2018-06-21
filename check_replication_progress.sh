#!/bin/bash
config_file=$1

saveIFS="$IFS"

nl=$'\n'

if [ -z $config_file ]
then
   echo "Some input variables are missed!"
   echo "Usage : check_replication_progress.sh [Path to config]"
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

saveIFS="$IFS"
nl=$'\n'

IFS=$'\n'
echo "Kudu_masters : $kudu_masters"
echo "Number_of_replicas : $number_of_replicas"
echo "Prefix : $prefix"
echo "Calling ksck..."
ksck=($(kudu cluster ksck $kudu_masters -checksum_scan))

IFS=$'\n'
echo "${ksck[@]/%/$'\n'}" > ksck_log.check_replication_progress

tablet_list=()
table_list=()
checksum_list=()
finded=0
IFS=$'\n'
for i in ${ksck[@]/%/$'\n'}
do
	#echo $i
	if [[ $i == *"-----------------------"* ]]
	then
		finded=$(((finded+1)%2))
	else
		if [[ $finded -eq 1 ]]
		then 
			table=$(echo "${i/ /}")
		else
			tablet_id=($(echo "$i" | grep "T " | awk 'BEGIN{FS=" "} {print $2}'))
			if [[ $tablet_id != "" ]]
			then 
				tablet_list+=($tablet_id)
				table_list+=($table)
				checksum_list+=($(echo "$i" | grep "T " | awk 'BEGIN{FS=" "} {print $7}'))
			fi
		fi 
	fi
done

# tablet_list=($(echo "${ksck[@]/%/$'\n'}" | grep "T " | awk 'BEGIN{FS=" "} {print $2}'))
# checksum_list=($(echo "${ksck[@]/%/$'\n'}" | grep "T " | awk 'BEGIN{FS=" "} {print $7}'))

# echo "${ksck[@]/%/$'\n'}"
# echo ${tablet_list[@]/%/$'\n'}
# echo ${checksum_list[@]}

echo "Number of replicas:" ${#tablet_list[@]}
echo "Number of related tables:" ${#table_list[@]}
echo "Number of checksums:" ${#checksum_list[@]}

# i=0
# while [[ i -lt ${#tablet_list[@]} ]]
# do 
	# echo ${tablet_list[$i]} ${table_list[$i]} ${checksum_list[$i]}
	# i=$((i+1))
# done

unset tablet_map
declare -A tablet_map
unset table_map
declare -A table_map

i=0
while [[ $i -lt ${#tablet_list[@]} ]]
do
	tablet_id=${tablet_list[$i]}
	checksum=${checksum_list[$i]}
	table_name=${table_list[$i]}
	table_name="${table_name/importdb./}"
	table_name="${table_name/migration./}"
	#echo "'$table_name' '$prefix'"
	if [ -n $prefix ]
	then
		if [[ $table_name == $prefix* ]]
		then
			tablet_map[$tablet_id]+=" $checksum"
			table_map[$tablet_id]="$table_name"
		fi
	else 
		tablet_map[$tablet_id]+=" $checksum"
		table_map[$tablet_id]="$table_name"
	fi
	i=$((i+1))
done

# for i in ${!tablet_map[@]};
# do 
	# echo $i ${tablet_map[$i]}
# done

unset error_tablet_map
declare -A error_tablet_map
IFS=' '
for i in ${!tablet_map[@]}
do
	table=${table_map[$i]}
	#echo "Table: ${table_map[$i]} Tablet: $i Checksum: ${tablet_map[$i]}"
	checksum_arr=(${tablet_map[$i]})
	#echo "checksum_arr:" ${checksum_arr[@]}
	finded_replicas_cnt=${#checksum_arr[@]}
	#echo "finded_replicas_cnt:" $finded_replicas_cnt
	if [[ $finded_replicas_cnt -ge $number_of_replicas ]]
	then
		#echo "finded_replicas_cnt >= $number_of_replicas"
		j=0
		while [[ $j -lt ${#checksum_arr[@]} ]]
		do
			if [[ $j -eq 0 ]]
			then
				first=${checksum_arr[$j]}
				if [[ $first == "Invalid" ]]; then
					error_tablet_map[$i]+="Table: $table Tablet_id: $i Error: Replica's state is 'Invalid'. Checksum: ${checksum_arr[@]}"
					#echo "Table: $table Tablet_id: $i Error: Replica's state is 'Invalid'. Checksum: ${checksum_arr[@]}"
					break
				fi
			else
				#echo $first
				if [[ $first != "${checksum_arr[$j]}" ]]; then
					error_tablet_map[$i]+="Table: $table Tablet_id: $i Error: Replicas' checksums not equal. Checksum: ${checksum_arr[@]}"
					#echo "Table: $table Tablet_id: $i Error: Replicas' checksums not equal. Checksum: ${checksum_arr[@]}"
					break
				fi
			fi
			j=$((j+1))
		done
	else
		error_tablet_map[$i]+="Table: $table Tablet_id: $i Error: not enough replicas. Checksum: ${checksum_arr[@]}"
		#echo "Table: $table Tablet_id: $i Error: not enough replicas. Checksum: ${checksum_arr[@]}"
	fi
done

IFS='\n'

echo "Number of tablets: ${#tablet_map[@]}"
echo "Number of tablets in invalid state: ${#error_tablet_map[@]}"
progress=$(lua -e "print(( ${#tablet_map[@]}-${#error_tablet_map[@]} )/${#tablet_map[@]}*100)")
echo "Progress: $progress"

if [[ ${#error_tablet_map[@]} -eq 0 ]];
then
	echo "Replication is done."
	echo "Status: OK"
else
	echo "See details from 'error_tablet_map':"
	for i in ${!error_tablet_map[@]}
	do 
		echo "${error_tablet_map[$i]}"
	done
	echo "Kudu needs more time for replication."
	echo "Status: ERROR"
fi

fi
