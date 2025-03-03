#!/bin/bash

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

echo "Processo concluído com sucesso!"
