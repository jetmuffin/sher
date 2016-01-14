#/bin/bash
a=1
while :
do
    a=$(($a+1))
    if test $a -gt 25
    then break
    else
    	echo $a
        echo $(ping -c 1 192.168.33.$a | grep "ttl" | awk '{print $4}'| sed 's/://g')
    fi
done