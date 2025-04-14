#!/bin/bash

# Check if the correct number of arguments is provided
if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <duration-in-seconds> <step-in-seconds> <scale-flag>"
  exit 1
fi

# Assign arguments to variables
duration=$1  # Duration to run the deployment process
step=$2      # Step for trace_collector.py
flag=$3      # States if we are comparing the experiment scenarios or emulating in a larger scale"

# Track start time
start_time=$(date +%s)

# # Function to clean up background processes when the script exits
# cleanup() {
#   echo "Stopping request_response_time.py..."
#   kill "$REQ_RESP_PID" 2>/dev/null
# }
# trap cleanup EXIT  # Ensure cleanup runs when the script finishes

# Setting dependencies up
echo "Installing dependencies: KWOK + Prometheus"
./bash_scripts/setup.sh

# Step 1: Run create_nodes.sh to create nodes
echo "Creating nodes..."
./bash_scripts/create_nodes.sh

# Step 2: Create Priority Classes
echo "Creating priority classes..."
./bash_scripts/create_priority_classes.sh

# # Step 3: Start request_response_time.py in the background
# echo "Starting request_response_time.py in the background..."
# python3 request_response_time.py &  
# REQ_RESP_PID=$!  # Capture its process ID (PID) for later termination

# Step 4: Deploy Pods
echo "Creating pod deployments..."
./bash_scripts/create_pods.sh "$duration" 

# Step 5: Port-forward Prometheus pod for data collection
kubectl port-forward --address 0.0.0.0 pod/prometheus-k8s-0 30222:9090 -n monitoring &

# Step 6: Wait for the specified duration minus elapsed time
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
remaining_time=$((duration - elapsed_time))

if ((remaining_time > 0)); then
  echo "Waiting for $remaining_time seconds before cleanup..."
  sleep "$remaining_time"
else
  echo "No remaining time left; skipping wait."
fi

# Step 7: Cleanup deployments and nodes
echo "Deleting nodes matching the pattern 'kwok-node-number'..."
for node in $(kubectl get nodes --no-headers | awk '{print $1}' | grep -E '^kwok-node-[0-9]+$'); do
  echo "Deleting node: $node"
  kubectl delete node "$node"
done

echo "Deleting all deployments..."
kubectl delete deployments --all

# ADICIONANDO A COLETA E PROCESSAMENTO DE MÉTRICAS

echo "Aguardando 10 segundos antes de coletar métricas..."
sleep 10

# Obtendo o nome do pod do scheduler
SCHPOD=$(kubectl get pods -n kube-system | grep "custom-scheduler" | awk '{print $1}')

# Criando diretório de saída com timestamp
OUTPUT_BASE="data/$(date +%Y%m%d_%H%M%S)_experiment"
mkdir -p "$OUTPUT_BASE"

echo "Copiando arquivos de métricas do scheduler para local..."
kubectl cp kube-system/$SCHPOD:/metrics.log "$OUTPUT_BASE/metrics.log"

# Copiando arquivos de latência e dados de serviço (se existirem)
cp $HOME/*.latency "$OUTPUT_BASE/" 2>/dev/null
cp $HOME/serviceapplication.dat "$OUTPUT_BASE/" 2>/dev/null

echo "Executando scripts R para análise de métricas..."
Rscript R/compute_final_availability.r "$OUTPUT_BASE/metrics.log" "$OUTPUT_BASE/final-controllers-qos.csv"
Rscript R/compute_makespan.r "$OUTPUT_BASE/metrics.log" "$OUTPUT_BASE/job-qos-metrics.csv"
Rscript R/compute_service_metrics.r "$OUTPUT_BASE/"

# Removendo arquivos temporários
rm $HOME/*.latency 2>/dev/null

echo "Processamento finalizado! Resultados disponíveis em $OUTPUT_BASE"

echo "Cluster cleanup completed."

# Step 8: Run trace_collector
#echo "Running trace_collector..."
#filepath=$(python3 trace_collector.py "$duration" "$step")
#echo "trace_collector execution completed."
#echo "$filepath"

# Step 9: Process the emulation results
#python3 process_emulation_result.py "$filepath"
#sleep 1 

# Step 10: Append results if scale-flag is false
#if [[ "$flag" == "false" ]]; then  
#  python3 append_results.py result-experiment/real_experiments_result.csv result-emulation/final_pod_data_processed.csv result-simulacao/simulation_experiment_result.csv
#fi

echo "All resources have been successfully created and trace collector has run!"

# Cleanup is automatically triggered here due to `trap EXIT`
