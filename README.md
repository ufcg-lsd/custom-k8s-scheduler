# QoS-Driven Update 1.31

## Descrição do Projeto

Este repositório contém o código atualizado para o sistema QoS-Driven Scheduler, desenvolvido para a versão 1.31 do Kubernetes. Ele busca otimizar a alocação de recursos e garantir a qualidade de serviço (QoS) através de estratégias avançadas de agendamento e preempção.

## Funcionalidades Atuais

### Status Atual

Atualmente o scheduler possui os plugins de PreBind e PostBind.

### Próximos Passos

Serão adicionados os plugins de QueueSort, Reserve e PostFilter.

## Comandos Úteis

### Acessar a VM

Para entrar na VM utilizar:

```bash
ssh -i chave ubuntu@150.165.85.20
```

### Criação de Imagem Docker

Para criar a imagem Docker:

```bash
docker build -t brunogb123/scheduler-custom:<especificar a Tag que será utilizada> .
```

**Observação**: Atualmente a tag é `v2`. Quando for implementado outro plugin, utilizar `v3`.

### Push para o Docker Hub

Não é necessário executar `push` para o Docker, tendo em vista que a imagem está no ambiente local. Caso seja necessário, utilize:

```bash
docker push brunogb123/scheduler-custom:<tag utilizada para o build da imagem>
```

### Enviar Imagem para o Minikube

Após criar a imagem, carregue-a para o Minikube:

```bash
minikube image load brunogb123/scheduler-custom:<tag utilizada para o build da imagem>
```

**Observação**: Caso queira apagar a imagem criada anteriormente, primeiro verifique o IMAGE ID utilizando:

```bash
docker images
```

Para apagar da VM:

```bash
docker rmi <imageid>
```

Para apagar de dentro do Minikube:

```bash
minikube ssh docker rmi <imageid>
```

### Configurar o ConfigMap

Atualmente o ConfigMap já está aplicado para definir os plugins utilizados. Caso seja necessário aplicar um novo, execute:

```bash
kubectl delete configmap custom-scheduler-config -n kube-system
kubectl create configmap custom-scheduler-config --from-file=yamls/scheduler-config.yaml -n kube-system
```

### Criar o Deployment do Scheduler

No diretório principal, execute:

```bash
kubectl apply -f yamls/custom-scheduler-deployment.yaml
```

### Aplicar YAML dos Pods

Para aplicar os YAMLs dos pods, utilize:

```bash
kubectl apply -f pods/test-pod.yaml
kubectl apply -f pods/test-pod2.yaml
```

### Apagar Deployments

Para apagar os deployments, execute:

```bash
kubectl delete deployment custom-scheduler -n kube-system
```

### Apagar Pods

Para apagar um pod, execute:

```bash
kubectl delete pod <nome-do-pod>
```

### Verificar Logs do Scheduler

Para verificar os logs do scheduler, utilize:

```bash
kubectl logs <nome-do-scheduler> -n kube-system --follow
```

**Observação**: Para checar o nome do scheduler caso não saiba, execute:

```bash
kubectl get pods -A
```

