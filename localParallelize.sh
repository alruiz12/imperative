#!/bin/bash
mkdir logs
for j in $(seq 1 $2); do
    mkdir logs/$j
    java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main $1 >logs/$j/A &>logs/$j/A &
    num="$(($1-1 ))"
    for i in $(seq 1 $num); do
        java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main2 $1 >logs/$j/$i &>logs/$j/$i &
    done

    FAIL=0
    for job in `jobs -p`
    do
        wait $job
    done

done

target="/home/alvaro/imperative/target/classes"
acc=0
n_times=0
for j in "$target"/*
do
	name=$( basename $j)
	if [[ $name == 'TIME='* ]]; then
		SUBSTRING=$( echo $name| cut -d'=' -f 2)
		acc="$(($acc+$SUBSTRING))"
		n_times="$(($n_times+1))"
	fi

done
echo $acc
div=1
div=$(echo 'scale=3; '$acc' / '$n_times |  bc )
echo $div