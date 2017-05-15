#!/bin/bash

name=("1.0" "0.8" "0.6" "0.4" "0.2" "0.0")

echo "<Cash bit (yes) : Skewness>"
echo

for i in ${name[@]}
do
   echo "[ get skewness = $i ]"
   (
   echo "l put2_$i.txt" 
   echo "l get2_$i.txt"
   echo "x"
   ) | ./cache_yes | grep "op/s" #< g get.txt
   echo
   sleep 0.5
done


echo "<Cash bit (no) : Skewness>"
echo

for i in ${name[@]}
do
   echo "[ get skewness = $i ]"
   (
   echo "l put2_$i.txt" 
   echo "l get2_$i.txt"
   echo "x"
   ) | ./cache_no | grep "op/s" #< g get.txt
   echo
   sleep 0.5
done

