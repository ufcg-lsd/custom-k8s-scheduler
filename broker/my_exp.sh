#!/bin/bash

set -e

bash my_experiment.sh workloads/all-except-one.csv 300 1 1 qos-driven

sleep 5