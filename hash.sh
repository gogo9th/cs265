#!/bin/bash

name=("5" "6" "7" "8" "9" "10" "11" "12")

echo "<Hash Table Size Ratio>"
echo

for i in ${name[@]}
do
	echo "[ hash table size ratio = 2^-$i ]"
	(
	echo "l put2.txt" 
	echo "l get2.txt"
	echo "x"
	) | ./hash_$i | grep "op/s" #< g get.txt
	echo
	sleep 3.0
done

