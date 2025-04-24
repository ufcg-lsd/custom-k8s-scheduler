#!/bin/bash

# Deleta o pod do scheduler
kubectl delete deployment custom-scheduler -n kube-system

# Limpa recursos do Docker
echo y | docker system prune

# Remove a imagem localmente
docker rmi <usuário-no-DockerHub>/<nome-da-imagem>:<tag>

# Remove a imagem dentro do Minikube
minikube ssh -- docker rmi <usuário-no-DockerHub>/<nome-da-imagem>:<tag>

# Constrói a nova imagem
docker build -t <usuário-no-DockerHub>/<nome-da-imagem>:<tag> .

# Carrega a imagem no Minikube
minikube image load <usuário-no-DockerHub>/<nome-da-imagem>:<tag>

#Aplica o yaml do scheduler
kubectl apply -f yamls/custom-scheduler-deployment.yaml 

echo "Processo concluído com sucesso!"
