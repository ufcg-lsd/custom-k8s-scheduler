#!/bin/bash

# Deleta Pods
kubectl delete pod high-priority-pod low-priority-pod

# Deleta o pod do scheduler
kubectl delete deployment custom-scheduler -n kube-system

# Limpa recursos do Docker
echo y | docker system prune

# Remove a imagem localmente
docker rmi brunogb123/scheduler-custom:bugs

# Remove a imagem dentro do Minikube
minikube ssh -- docker rmi brunogb123/scheduler-custom:bugs

# Constrói a nova imagem
docker build -t brunogb123/scheduler-custom:bugs .

# Carrega a imagem no Minikube
minikube image load brunogb123/scheduler-custom:bugs

#Aplica o yaml do scheduler
kubectl apply -f yamls/custom-scheduler-deployment.yaml 

echo "Processo concluído com sucesso!"
