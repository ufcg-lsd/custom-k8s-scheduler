#!/bin/bash

# Check if the correct number of arguments is provided
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <duration-in-seconds>"
  exit 1
fi

# Assign arguments to variables
duration=$1

# Hardcoded order of workload classes, repeated 20 times
base_order=(bronze gold bronze gold bronze silver gold gold gold bronze bronze gold bronze gold gold bronze gold bronze bronze bronze silver silver gold bronze
gold bronze silver silver silver gold silver bronze silver gold silver bronze bronze bronze bronze silver silver silver bronze silver gold bronze gold gold
gold bronze bronze gold silver silver bronze gold gold silver bronze gold silver silver gold gold bronze bronze gold bronze silver bronze bronze silver gold
bronze silver gold gold silver silver gold gold gold gold silver gold silver bronze gold silver gold silver silver gold bronze bronze silver gold silver
silver gold bronze silver gold bronze silver bronze silver bronze silver bronze silver gold gold bronze silver silver gold bronze silver silver silver bronze
bronze bronze bronze gold silver silver silver bronze gold silver bronze silver bronze silver bronze gold silver bronze gold silver silver silver bronze gold
silver gold bronze silver bronze bronze bronze bronze bronze gold bronze gold gold gold gold gold bronze bronze bronze silver gold bronze bronze silver silver
bronze silver gold bronze bronze silver gold silver bronze bronze silver bronze gold gold bronze silver gold gold gold bronze bronze gold silver gold bronze
silver gold silver bronze gold silver bronze silver silver gold silver bronze gold silver bronze gold bronze bronze silver bronze gold bronze bronze gold bronze
bronze bronze gold bronze gold gold silver bronze silver gold bronze gold gold gold silver silver gold silver silver bronze bronze bronze silver bronze bronze
bronze gold bronze bronze silver gold bronze silver bronze silver)

# Repeat the base order 20 times
order=()
for ((i=0; i<1; i++)); do
  order+=("${base_order[@]}")
done

order=(
bronze bronze gold gold gold gold bronze gold bronze gold
bronze bronze bronze gold gold gold gold gold gold
bronze gold gold gold gold gold gold gold gold gold bronze
gold bronze bronze gold bronze gold gold bronze gold
gold gold bronze gold gold gold bronze gold gold
bronze bronze gold gold gold gold bronze gold gold gold
bronze gold bronze bronze gold gold gold
bronze bronze bronze bronze gold gold gold gold
bronze bronze gold bronze bronze bronze bronze gold gold gold
bronze gold bronze gold bronze gold bronze bronze gold gold
gold gold bronze gold gold bronze
bronze bronze bronze bronze bronze gold bronze gold
gold bronze bronze gold gold bronze bronze gold bronze gold
bronze gold gold gold bronze gold bronze gold gold
bronze gold bronze gold gold bronze bronze gold gold bronze bronze gold
bronze bronze bronze bronze bronze bronze gold gold gold gold gold bronze
bronze bronze bronze bronze bronze bronze bronze gold gold gold bronze bronze
gold gold gold bronze gold bronze gold gold bronze gold
bronze gold bronze gold gold gold bronze gold
)

# Map workload classes to priority classes
declare -A priority_classes=(
  ["gold"]="gold-priority"
  ["bronze"]="bronze-priority"
  ["silver"]="silver-priority"
)

# Function to clean up background processes when the script exits
cleanup() {
  echo "Stopping request_response_time.py..."
  pkill -f request_response_time.py
}
trap cleanup EXIT  # Ensure cleanup runs when the script finishes

# Start request_response_time.py **only once** in the background
echo "Starting request_response_time.py (it will log once every batch of 20 deployments)..."
python3 request_response_time.py &
REQ_RESP_PID=$!  # Capture its process ID for cleanup

# Track the start time of the deployment process
start_time=$(date +%s)
deployments_in_batch=0
deployment_index=1  # Single counter for all deployments

# Iterate through the entire order array
for class in "${order[@]}"; do
  # Define o valor de slo_value de acordo com a classe atual
  if [[ $class == "gold" ]]; then
    slo_value="1"
  elif [[ $class == "silver" ]]; then
    slo_value="0.9"
  elif [[ $class == "bronze" ]]; then
    slo_value="0.5"
  else
    slo_value="0.5"  # Valor padrão, se necessário
  fi
  
  # Get the priority class for the workload class
  priority_class=${priority_classes[$class]}

  # Validate the class
  if [[ -z $priority_class ]]; then
    echo "Unknown workload class $class. Skipping deployment..."
    continue
  fi

  # Creating the deployment YAML dynamically and applying it
  echo "Creating pod ${class}-class-deploy-$deployment_index for $class with priority $priority_class and slo $slo_value..."
  kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${class}-class-deploy-$deployment_index
  labels:
    app: ${class}-class-app-$deployment_index
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${class}-class-app-$deployment_index
  template:
    metadata:
      annotations:
        controller: "${class}-controller-${deployment_index}"
        qosMeasuring: "average"
        acceptablePreemptionOverhead: "0.05"
        slo: "${slo_value}"
        kwok.x-k8s.io/usage-cpu: 1m
        kwok.x-k8s.io/usage-memory: 1Mi
      labels:
        app: ${class}-class-app-$deployment_index
    spec:
      terminationGracePeriodSeconds: 0
      schedulerName: custom-scheduler
      priorityClassName: ${priority_class}
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: kwok
                operator: In
                values:
                - "true"
      containers:
      - name: ${class}-class-container-$deployment_index
        image: fake-image
        resources:
          requests:
            cpu: "200m"
            memory: "200Mi"
          limits:
            cpu: "200m"
            memory: "200Mi"
EOF

  # Increment the deployment index
  ((deployment_index++))

  # Track number of deployments per batch
  ((deployments_in_batch++))

  # Run the `kubectl get pod` response time script **once per batch of 20**
  if (( deployments_in_batch == 20 )); then
    echo "Batch of 20 deployments applied. Logging response time..."
    kill -SIGUSR1 "$REQ_RESP_PID"  # Signal the background process to log
    echo "Logged response time. Waiting for 1 second before next batch..."
    sleep 1
    deployments_in_batch=0  # Reset counter
  fi
done

echo "Pods creation process completed!"