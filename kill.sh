kill $(ps -eo pid | awk '$1 >= $PIDS')
rm /dev/shm/sem.*
