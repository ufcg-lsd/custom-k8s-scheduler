kubectl create namespace monitoring

kubectl apply --server-side -f kube-prometheus/manifests/setup
kubectl wait \
	--for condition=Established \
	--all CustomResourceDefinition \
	--namespace=monitoring
kubectl apply -f kube-prometheus/manifests/
kubectl wait --for=condition=Ready pods --all --namespace=monitoring --timeout=300s

# ID 6417
# Access Prometheus
# Access Grafana
# kubectl port-forward --address 0.0.0.0 pod/my-prometheus-grafana-{hash}  3000:3000 -n monitoring &
# kubectl get secret my-prometheus-grafana -n monitoring -o jsonpath="{.data.admin-password}" | base64 --decode; echo
