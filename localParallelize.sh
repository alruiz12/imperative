#!/bin/bash
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main >A &>A &
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main3 >B &>B &
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main3 >C &>C &
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main3 >D &>D &
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main3 >E &>E &
java -cp /home/alvaro/.m2/repository/com/hazelcast/hazelcast/3.8.6/hazelcast-3.8.6.jar:/home/alvaro/Downloads/hazelcast-3.8.6/lib/hazelcast-client-3.8.6.jar:. main3 >F &>F &

