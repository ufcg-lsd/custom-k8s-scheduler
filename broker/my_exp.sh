#!/bin/bash

set -e

#bash my_experiment.sh workloads/all-except-one.csv 900 1 1 qos-driven

#bash my_experiment.sh workloads/half-running.csv 300 1 1 qos-driven

bash my_experiment.sh workloads/twoPods.csv 1800 1 1 qos-driven

sleep 5