#!/bin/bash
INPUT_FILE=$1
DURATION=$2
SERVICE_RATE_ATTACK=$3
FACTORIAL_NUMBER=$4
SCHEDULER=$5

INPUT_BASE=`basename -- $INPUT_FILE | sed 's/.csv//g'`
#OUTPUT_BASE=`date +%Y%m%d"_"%H%M%S`"_"$INPUT_BASE
OUTPUT_BASE=`date +%Y%m%d"_"%H%M%S`"_"$SCHEDULER"_"$INPUT_BASE

mkdir -p data/$OUTPUT_BASE

rm -f $HOME/*.latency

rm -f $HOME/serviceapplication.dat
#cp /dev/null /home/serviceapplication.dat

# go run broker.go $INPUT_FILE $DURATION $SERVICE_RATE_ATTACK $FACTORIAL_NUMBER
go run broker-exp-runtime-bkp.go $INPUT_FILE $DURATION $SERVICE_RATE_ATTACK $FACTORIAL_NUMBER
#go run broker-exp.go $INPUT_FILE $DURATION $SERVICE_RATE_ATTACK $FACTORIAL_NUMBER 1800 5

SCHPOD=`kubectl get pods -n kube-system | grep -v NAME  | grep "custom-scheduler" | awk '{print $1}'`

sleep 10

AGGR_TIME=0
COUNTER=`kubectl get pods | grep -v resources | wc -l`
MAX_DURATION=240
while [  $COUNTER -ne 0 ]; do
        sleep 1
        AGGR_TIME=$((AGGR_TIME+1))
        COUNTER=`kubectl get pods | grep -v resources | wc -l`

        echo "waiting for "$AGGR_TIME" seconds."
        if [ $AGGR_TIME -gt $MAX_DURATION ]
        then
                echo "Deleting all deployments, services and jobs from default namespace"
                kubectl delete deployment --all --namespace=default
                kubectl delete services --all --namespace=default
	        kubectl delete jobs --all --namespace=default
        fi
done

echo "Experiment execution is terminating."
echo "We will copy the result file from scheduler to local storage."

sleep 10

kubectl cp kube-system/$SCHPOD:/metrics.log data/$OUTPUT_BASE/metrics.log
cp $HOME/*.latency data/$OUTPUT_BASE/

cp $HOME/serviceapplication.dat data/$OUTPUT_BASE/

echo "Output base directory: data/$OUTPUT_BASE"


sleep 1

Rscript ../R/compute_final_availability.r data/$OUTPUT_BASE/metrics.log data/$OUTPUT_BASE/final-controllers-qos.csv

Rscript ../R/compute_makespan.r data/$OUTPUT_BASE/metrics.log data/$OUTPUT_BASE/job-qos-metrics.csv

Rscript ../R/compute_service_metrics.r data/$OUTPUT_BASE/

## probably to be used with new broker-exp.go file
##Rscript ../R/compute_service_metrics_diff_moments.r data/$OUTPUT_BASE/
#
##kubectl cp /dev/null kube-system/$SCHPOD:/metrics.log
rm $HOME/*.latency

