#!/bin/bash

set -e

#bash my_experiment.sh workloads/all-except-one.csv 900 1 1 qos-driven

#bash my_experiment.sh workloads/half-running.csv 300 1 1 qos-driven

#bash my_experiment.sh workloads/twoPods.csv 300 1 1 qos-driven

#bash my_experiment.sh workloads/four-pods.csv 300 1 1 qos-driven

#bash my_experiment.sh workloads/ericExperiment.csv 3600 1 1 qos-driven

#bash my_experiment.sh workloads/128pods.csv 3600 1 1 qos-driven

bash my_experiment.sh workloads/64pods.csv 3600 1 1 qos-driven

#bash my_experiment.sh workloads/32pods.csv 3600 1 1 qos-driven

sleep 5


