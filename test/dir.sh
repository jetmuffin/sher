#!/bin/bash
for i in `find /home/vagrant -name *txt`; 
	do echo $i; 
	cat $i; 
done