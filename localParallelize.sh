#!/bin/bash
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main $1 >A &>A &
num="$(($1-1 ))"
for i in $(seq 1 $num); do
    java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main3 $1 >$i &>$i &
    echo $i
done

