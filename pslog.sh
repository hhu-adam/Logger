#!/bin/bash
process=$1
pm2=/home/adam/.nvm/versions/node/v22.6.0/bin/pm2
pid=$( /home/adam/.nvm/versions/node/v22.6.0/bin/pm2 pid $process )

process_path=$PS_LOG"/"$process

if [ ! -d "$process_path" ]
then
    mkdir "$process_path"
fi

if [ ! -f $process_path"/"$process".log" ] 
then
    top -b -n 1 -p $pid | awk 'NR > 6 {$12=substr($0,72); print $1";"$2";"$3";"$4";"$5";"$6";"$7";"$8";"$9";"$10";"$11";"$12}' >> $process_path"/"$process".log"
else
    top -b -n 1 -p $pid | awk 'NR > 6 {$12=substr($0,72); print $1";"$2";"$3";"$4";"$5";"$6";"$7";"$8";"$9";"$10";"$11";"$12}' | sed -n '2 p' >> $process_path"/"$process".log"    
fi

