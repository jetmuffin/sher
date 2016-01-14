#!/bin/bash

########################################################################
#This scripts is checking the i/o stat,vision 0.1Bate
#  Write by finddream
#If you have some advise about it ,you can mail :finddream863@163.com
########################################################################

#make the local language is chinese
export LANG=zh_CN

#make the around command path

ECHO=/bin/echo
SED=/bin/sed
AWK=/bin/awk
UPTIME=/bin/uptime
VMSTAT=/usr/bin/vmstat
FREE=/usr/bin/free
IPTABLES=/sbin/iptables
GREP=/bin/grep
TOP=/usr/bin/top
HEAD=/usr/bin/head
DF=/bin/df
CAT=/bin/cat

#setup the time of the check

DATE=`/bin/date +%c`
$ECHO "   "
$ECHO "   "
$ECHO "本次检测的时间是$DATE"
$ECHO "---------------------------------------------------------------------------------------------"

#check the cpu stat

$ECHO "当前时刻CPU使用状况如下："
$ECHO "`$TOP -n 1 |$GREP  Cpu`" 
$ECHO "---------------------------------------------------------------------------------------------" 

#check the memory stat

$ECHO "当前时刻内存占用情况如下："
$ECHO "`$FREE |$GREP  -1 Mem |$HEAD -n 2 `"
$ECHO "----------------------------------------------------------------------------------------------" 

#check the disk stat

$ECHO "当前时刻磁盘空间使用情况如下："
$ECHO "`$DF -h `"
$ECHO "----------------------------------------------------------------------------------------------" 

#check the network stat

NETWORK_STAT=/proc/net/dev
$ECHO "当前时刻网络流量统计如下："
$ECHO "`$CAT $NETWORK_STAT|$GREP -v lo |$GREP -v sit0 `"
$ECHO "##################################################################################################################################" 

exit 0