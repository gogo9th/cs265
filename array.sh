echo

#name=("4kb" "16kb" "64kb" "256kb" "1mb" "4mb" "16mb" "64mb" "128mb")

name=("4kb" "16kb" "64kb" "256kb" "1mb")

echo "<Array>"
echo

for i in ${name[@]}
do 
   echo "[ buffer = $i ]"
   (
   echo "l put.txt" 
   echo "l get.txt"
   ) | ./array_$i | grep "op/s" #< g get.txt
   echo
   sleep 1
done


