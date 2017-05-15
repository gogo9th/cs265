#!/bin/bash

name=("0.0" "0.2" "0.4" "0.6" "0.8" "1.0")

echo "<Cash bit (yes) : Get-Miss Ratio>"
echo

for i in ${name[@]}
do
	echo "[ get miss rate = $i ]"
	(
	echo "l put$i.txt" 
	echo "l get$i.txt"
	echo "x"
	) | ./cache_yes | grep "op/s" #< g get.txt
	echo
	sleep 0.5
done


echo "<Cash bit (no) : Get-Miss Ratio>"
echo

for i in ${name[@]}
do
	echo "[ get miss rate = $i ]"
	(
	echo "l put$i.txt" 
	echo "l get$i.txt"
	echo "x"
	) | ./cache_no | grep "op/s" #< g get.txt
	echo
	sleep 0.5
done

