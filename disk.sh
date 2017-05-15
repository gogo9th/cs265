#!/bin/bash

name=("11" "9" "7" "5" "3" "1")

echo "<Disk Hash Partitions>"
echo

for i in ${name[@]}
do
	echo "[ hash-partitions= $i ]"
	(
	echo "l put2.txt" 
	echo "l get2.txt"
	echo "x"
	) | ./disk_$i | grep "op/s" #< g get.txt
	echo
	sleep 3
done

