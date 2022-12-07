#! /bin/bash

source $ISFS/scripts/isfs_functions.sh
set -x
set_project CFACT noqc_geo
export NETCDF_DIR=.
export NC_SERVER=localhost

export LD_LIBRARY_PATH=/opt/local/nc_server/lib64:${LD_LIBRARY_PATH} 
valgrind /opt/local/nc_server/bin/nc_server -d -l 7 -p 0 -s >& vg.log &
ncpid="$!"
echo server pid $ncpid
while true; do
    sleep 2
    port=`egrep PORT vg.log`
    if [ -n "$port" ]; then
        echo $port
        eval "export $port"
        break
    fi
done
logconfig=" --log debug --log file=StatisticsCruncher,verbose,enable --log file=DOMObjectFactory,disable --log file=Project,disable --log file=dynld/isff,disable --logfields level,function,message --logparam stats_log_variable_name=."
logconfig=" --log debug --log file=DOMObjectFactory,disable --log file=Project,disable --log file=dynld/isff,disable --logfields level,function,message --logparam stats_log_variable_name=."
begin="2022 jan 8 16:00:00"
end="2022 jan 8 16:15:00"
data=~/Data/projects/CFACT/raw_data/isfs_20220108_160000.dat.bz2
statsproc -B "$begin" -E "$end" $data -p 300 -H barolo $logconfig $when >& stats.log

# try to shutdown nc_server after statsproc run is done
for attempt in 1 2 3 ; do

    kill -0 $ncpid || break

    case "$attempt" in

        1)
            nc_close $NC_SERVER
            nc_shutdown $NC_SERVER
            ;;
        2)
            kill $ncpid
            ;;
        3)
            kill -9 $ncpid
            ;;
    esac
    sleep 5

done
