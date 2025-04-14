#!/bin/bash

# Define priority classes and their values
declare -A priority_classes=(
  ["gold-priority"]=2000
  ["silver-priority"]=1500
  ["bronze-priority"]=1000
)

# Loop through each priority class and apply it
for priority in "${!priority_classes[@]}"; do
  value=${priority_classes[$priority]}
  
  kubectl apply -f - <<EOF
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: ${priority}
value: ${value}
globalDefault: false
description: "${priority^} for critical workloads."
EOF

  echo "Created PriorityClass: ${priority} with value ${value}"
done
