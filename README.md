# 🚀 QoS-Driven Scheduler para Kubernetes 1.32

Scheduler customizado para Kubernetes com foco em alocação de recursos baseada em **Qualidade de Serviço (QoS)**.

---

## 📂 Estrutura do Projeto

- **`broker/`**: Testes locais utilizando nós e pods reais.
- **`cluster/`**: Yamls para classes de prioridade e read-jobs.
- **`kwok-experiment/`**: Ambiente de emulação com [KWOK](https://github.com/kubernetes-sigs/kwok).
- **`pkg/`**: Implementação dos plugins principais do scheduler QoS-Driven.
- **`R/`**: Scripts para análise de dados.
- **`yamls/`**: Arquivos de configuração do escalonador.
- **`build_and_load`**: Script para build e carga da imagem Docker.
- **`main.go`**: Arquivo principal de execução do escalonador.

---

## ⚙️ Pré-requisitos

- Kubernetes `v1.32`
- [Minikube](https://minikube.sigs.k8s.io/)
- [Docker](https://www.docker.com/)

---

## 🚀 Instalação Rápida

**Aplicar o ConfigMap do scheduler:**

```bash
kubectl apply -f yamls/scheduler-config.yaml -n kube-system
```

**Atualizar configuração (se necessário):**

```bash
kubectl delete configmap custom-scheduler-config -n kube-system
```

```bash
kubectl create configmap custom-scheduler-config --from-file=yamls/scheduler-config.yaml -n kube-system
```

**Build, carga e aplicação da imagem Docker:**

```bash
bash build_and_load
```

---

## 🧪 Experimento com KWOK (kwok-experiment-sbrc)

Para executar um experimento com KWOK, navegue até o diretório `kwok-experiment-sbrc` e execute:

```bash
bash run.sh <tempo de execução em segundos> 30 true
```

Os resultados estaram presentes dentro do diretório diretório `kwok-experiment-sbrc/data`

---

## 🛠️ Comandos Úteis

**Listar imagens Docker:**

```bash
docker images
```

**Remover imagem local Docker:**

```bash
docker rmi <image-id>
```

**Remover imagem Docker do Minikube:**

```bash
minikube ssh docker rmi <image-id>
```

**Acompanhar logs do scheduler:**

```bash
kubectl logs <scheduler-pod> -n kube-system --follow
```

**Reiniciar o scheduler:**

```bash
kubectl rollout restart deployment/custom-scheduler -n kube-system
```
