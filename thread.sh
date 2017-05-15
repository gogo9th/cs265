#!/bin/bash

name=("64" "32" "16" "8" "4" "2" "1")

echo "<Threads>"
echo

for i in ${name[@]}
do
	echo "[ threads = $i ]"
	(
	echo "l put2.txt" 
	echo "l get2.txt"
	echo "x"
	) | ./thread_$i | grep "op/s" #< g get.txt
	echo
	sleep 2
done

