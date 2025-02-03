package qos_driven_scheduler

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

const (
	Name                                = "QosAware"
	DefaultSlo                          = 0.80
	DefaultAcceptablePreemptionOverhead = 1.00
)

// QosDrivenScheduler implementa as interfaces de plugins solicitadas
type QosDrivenScheduler struct {
	fh          framework.Handle
	Args        QosDrivenSchedulerArgs
	PodInformer cache.SharedIndexInformer
	Controllers map[string]ControllerMetricInfo
	lock        sync.RWMutex
	PodLister   corelisters.PodLister
}

// Garantindo que QosDrivenScheduler implementa as interfaces necessárias
var _ framework.QueueSortPlugin = &QosDrivenScheduler{}  //Feito
var _ framework.ReservePlugin = &QosDrivenScheduler{}    // Feito
var _ framework.PreBindPlugin = &QosDrivenScheduler{}    // Feito
var _ framework.PostBindPlugin = &QosDrivenScheduler{}   // Feito
var _ framework.PostFilterPlugin = &QosDrivenScheduler{} //Feito

// Name retorna o nome do plugin
func (scheduler *QosDrivenScheduler) Name() string {
	return Name
}

// QueueSortPlugin: Determina a ordem de prioridade dos pods na fila
func (scheduler *QosDrivenScheduler) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	klog.Infof("[QueueSort] Comparando prioridade de %s com %s", pInfo1.Pod.Name, pInfo2.Pod.Name)
	if pInfo1.Pod.Name == pInfo2.Pod.Name && pInfo1.Pod.Namespace == pInfo2.Pod.Namespace {
		klog.Warningf("[QueueSort] Tentativa de comparar o pod %s consigo mesmo. Ignorando comparação.", pInfo1.Pod.Name)
		return false
	}
	precedence := scheduler.HigherPrecedence(pInfo1.Pod, pInfo2.Pod)
	klog.Infof("[QueueSort] Precedência determinada: %s tem precedência sobre %s? %v", pInfo1.Pod.Name, pInfo2.Pod.Name, precedence)
	return precedence
}

func (scheduler *QosDrivenScheduler) CompareInformerAndRealState(pod *corev1.Pod) {
	realPod, err := scheduler.fh.ClientSet().CoreV1().Pods(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Erro ao buscar estado real do pod %s: %v", pod.Name, err)
		return
	}
	klog.Infof("Estado no Informer: %+v", pod.Annotations)
	klog.Infof("Estado real no cluster: %+v", realPod.Annotations)
}

// We calculate precedence between pods checking if the time to violate p1's controller SLO is lower than p2's controller.
// If the pods' controllers has different importances and their time to violate are below the safety margin the precedence is for the highest importance controller's pod.
func (scheduler *QosDrivenScheduler) HigherPrecedence(p1, p2 *corev1.Pod) bool {
	// Verificação para evitar comparar o pod consigo mesmo
	if p1.Name == p2.Name && p1.Namespace == p2.Namespace {
		klog.Warningf("[HigherPrecedence] Tentativa de comparar o pod %s consigo mesmo. Retornando false.", p1.Name)
		return false
	}

	now := time.Now()
	klog.Infof("[HigherPrecedence] Calculando precedência entre %s e %s", p1.Name, p2.Name)

	scheduler.WaitForPodsOnCache(p1, p2)
	cMetricInfo1 := scheduler.GetControllerMetricInfo(p1)
	cMetrics1 := cMetricInfo1.Metrics(now, scheduler.lock.RLocker())
	cMetricInfo2 := scheduler.GetControllerMetricInfo(p2)
	cMetrics2 := cMetricInfo2.Metrics(now, scheduler.lock.RLocker())

	importance1 := ControllerImportance(p1)
	importance2 := ControllerImportance(p2)

	qosMetric1 := cMetrics1.QoSMetric(p1)
	qosMetric2 := cMetrics2.QoSMetric(p2)

	klog.Infof("[HigherPrecedence] Métricas para %s: QoS = %f, Importância = %.2f", p1.Name, qosMetric1, importance1)
	klog.Infof("[HigherPrecedence] Métricas para %s: QoS = %f, Importância = %.2f", p2.Name, qosMetric2, importance2)

	safetyMargin := scheduler.Args.SafetyMargin.Duration.Seconds()

	// Is in resource contention
	if (qosMetric1 < safetyMargin) && (qosMetric2 < safetyMargin) && importance1 != importance2 {
		klog.Infof("[HigherPrecedence] Ambos estão em contenção de recursos e têm diferentes importâncias.")
		return importance1 > importance2
	}

	result := qosMetric1 < qosMetric2
	klog.Infof("[HigherPrecedence] Decisão final: %s tem precedência sobre %s? %v", p1.Name, p2.Name, result)
	return result
}

// TODO maybe we should sort only by controller's SLO and importance if we can't find its metrics
func (scheduler *QosDrivenScheduler) WaitForPodsOnCache(pods ...*corev1.Pod) {
	for _, pod := range pods {
		klog.Infof("[WaitForPodsOnCache] Verificando se o pod %s está no cache...", pod.Name)
		cacheMiss := func() bool {
			cMetricInfo := scheduler.GetControllerMetricInfo(pod)
			scheduler.lock.RLock()
			defer scheduler.lock.RUnlock()

			_, notFound := cMetricInfo.GetPodMetricInfo(pod)
			if notFound {
				klog.Warningf("[cacheMiss] Métricas não encontradas para o pod: %s/%s", pod.Namespace, pod.Name)
			}

			// Comparar o estado real com o estado no cache
			scheduler.CompareInformerAndRealState(pod)

			return notFound
		}

		for cacheMiss() {
			klog.Warningf("[WaitForPodsOnCache] Pod %s não encontrado no cache. Tentando novamente...", pod.Name)
			time.Sleep(time.Millisecond * 100)
		}

		klog.Infof("[WaitForPodsOnCache] Pod %s encontrado no cache.", pod.Name)
	}
}

// ReservePlugin: Chamado quando os recursos são reservados
func (scheduler *QosDrivenScheduler) Reserve(_ context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	now := time.Now()
	klog.Infof("[Reserve] resources reserved at node %s for pod %s at %s", nodeName, PodName(pod), now)
	scheduler.UpdatePodMetricInfo(pod, func(old PodMetricInfo) PodMetricInfo {
		klog.Infof("[Reserve] Status de alocação atual para o pod %s: %v", PodName(pod), old.AllocationStatus)
		old.AllocationStatus = AllocatingState
		klog.Infof("[Reserve] Status de alocação atualizado para o pod %s: %v", PodName(pod), AllocatingState)
		return old
	})
	return nil
}

// ReservePlugin: Chamado quando a reserva é desfeita
func (scheduler *QosDrivenScheduler) Unreserve(_ context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) {
	now := time.Now()
	klog.Infof("[Unreserve] Recursos estão sendo liberados no node %s para o pod %s às %s", nodeName, PodName(pod), now)

	klog.Infof("[Unreserve] Atualizando métricas do pod %s para liberar recursos", PodName(pod))
	scheduler.UpdatePodMetricInfo(pod, func(old PodMetricInfo) PodMetricInfo {
		klog.Infof("[Unreserve] Estado anterior do AllocationStatus para pod %s: %v", PodName(pod), old.AllocationStatus)
		old.AllocationStatus = ""
		klog.Infof("[Unreserve] Novo estado do AllocationStatus para pod %s: %v", PodName(pod), old.AllocationStatus)
		return old
	})

	klog.Infof("[Unreserve] Recursos liberados com sucesso para o pod %s no node %s", PodName(pod), nodeName)
}

// PreBindPlugin: Chamado antes de o pod ser vinculado ao node
func (scheduler *QosDrivenScheduler) PreBind(_ context.Context, state *framework.CycleState, p *corev1.Pod, nodeName string) *framework.Status {
	klog.Infof("[PreBind] Validando o pod %s antes de vincular ao node %s", p.Name, nodeName)
	now := time.Now()
	state.Write(BindingStart, CloneableTime{now})
	return nil
}

// PostBindPlugin: Chamado após o pod ser vinculado ao node
func (scheduler *QosDrivenScheduler) PostBind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, _ string) {
	klog.Infof("[PostBind] Chamado para pod %s", pod.Name)

	// Tenta ler o estado `BindingStart`
	start, err := state.Read(BindingStart)
	if err != nil {
		klog.Errorf("[PostBind] Erro ao ler BindingStart para pod %s: %v", pod.Name, err)
		return
	}

	// Verifica o tipo do valor retornado
	startBinding, ok := start.(CloneableTime)
	if !ok {
		klog.Errorf("[PostBind] Tipo inesperado para BindingStart no pod %s", pod.Name)
		return
	}

	// Calcula o tempo de binding
	endBinding := time.Now()
	klog.Infof("[PostBind] Pod %s - Início do binding: %s, Fim do binding: %s", pod.Name, startBinding.Time, endBinding)

	// Atualiza as métricas do pod
	scheduler.UpdatePodMetricInfo(pod, func(old PodMetricInfo) PodMetricInfo {
		old.StartBindingTime = startBinding.Time
		old.StartRunningTime = endBinding
		old.AllocationStatus = AllocatedState
		klog.Infof("[PostBind] Métricas atualizadas para o pod %s", pod.Name)
		return old
	})
}

func (scheduler *QosDrivenScheduler) OnAddPod(obj interface{}) {

	p := obj.(*corev1.Pod).DeepCopy()
	klog.Infof("[OnAddPod] Pod adicionado: %s/%s, Anotações: %+v", p.Namespace, p.Name, p.Annotations)

	// Comparar o estado no Informer com o estado real no cluster
	scheduler.CompareInformerAndRealState(p)

	if p.Namespace == "kube-system" {
		klog.Infof("[OnAddPod] Ignorando pod %s/%s pois pertence ao namespace kube-system", p.Namespace, p.Name)
		return
	}

	start := p.GetCreationTimestamp().Time
	klog.Infof("[OnAddPod] Adicionando pod %s/%s com os seguintes detalhes:\n%s", p.Namespace, p.Name, p.String())

	// CreationTimestamp is marked as +optional in the API, so we put this fallback
	if start.IsZero() {
		start = time.Now()
		klog.Infof("[OnAddPod] Timestamp de criação não definido para o pod %s/%s, usando o horário atual: %v", p.Namespace, p.Name, start)
	}

	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
		klog.Infof("[OnAddPod] Atualizando métricas para o pod %s/%s", p.Namespace, p.Name)
		pMetricInfo.ControllerName = ControllerName(p)
		pMetricInfo.LastStatus = p
		pMetricInfo.CreationTime = start

		// pod starts running at this moment
		if pMetricInfo.StartRunningTime.IsZero() && p.Status.Phase == corev1.PodRunning {
			pMetricInfo.StartRunningTime = time.Now()
			klog.Infof("[OnAddPod] Pod %s/%s entrou no estado Running", p.Namespace, p.Name)
		}

		return pMetricInfo
	})
	klog.Infof("[OnAddPod] Pod %s/%s adicionado com sucesso", p.Namespace, p.Name)
}

func (scheduler *QosDrivenScheduler) OnUpdatePod(_, newObj interface{}) {

	klog.Infof("[OnUpdatePod] Iniciando processamento de atualização de pod")

	// Criando uma cópia do pod atualizado
	p := newObj.(*corev1.Pod).DeepCopy()
	klog.Infof("[OnUpdatePod] Pod atualizado detectado: %s/%s, Anotações: %+v", p.Namespace, p.Name, p.Annotations)

	// Comparando o estado no Informer com o estado real no cluster
	klog.Infof("[OnUpdatePod] Comparando estado no Informer e estado real no cluster para o pod %s/%s", p.Namespace, p.Name)
	scheduler.CompareInformerAndRealState(p)

	// Ignorando pods no namespace kube-system
	if p.Namespace == "kube-system" {
		klog.Infof("[OnUpdatePod] Pod %s/%s pertence ao namespace 'kube-system'. Ignorando...", p.Namespace, p.Name)
		return
	}

	// Atualizando informações de métricas do pod
	klog.Infof("[OnUpdatePod] Atualizando informações de métricas para o pod %s/%s", p.Namespace, p.Name)
	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
		klog.Infof("[OnUpdatePod] Atualizando 'LastStatus' do pod %s/%s nas métricas", p.Namespace, p.Name)
		pMetricInfo.LastStatus = p
		klog.Infof("[OnUpdatePod] Métrica atualizada para o pod %s/%s", p.Namespace, p.Name)
		return pMetricInfo
	})

	klog.Infof("[OnUpdatePod] Finalizando processamento de atualização de pod para %s/%s", p.Namespace, p.Name)
}

func (scheduler *QosDrivenScheduler) OnDeletePod(lastState interface{}) {

	klog.Infof("[OnDeletePod] Iniciando a execução do OnDeletePod")

	p, ok := lastState.(*corev1.Pod)
	if !ok {
		klog.Warningf("[OnDeletePod] O estado anterior do pod deletado é desconhecido")
		return
	}

	p = p.DeepCopy()
	klog.Infof("[OnDeletePod] DeepCopy realizado para o pod %s/%s", p.Namespace, p.Name)

	if p.Namespace == "kube-system" {
		klog.Infof("[OnDeletePod] Pod %s/%s pertence ao namespace 'kube-system'. Ignorando...", p.Namespace, p.Name)
		return
	}

	klog.Infof("[OnDeletePod] Pod deletado detectado: %s/%s\nDetalhes do pod: %s", p.Namespace, p.Name, p.String())

	klog.Infof("[OnDeletePod] Atualizando métricas do pod %s/%s", p.Namespace, p.Name)
	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
		klog.Infof("[OnDeletePod] Verificando o estado do pod %s/%s nas métricas", p.Namespace, p.Name)

		// if pod was succeeded terminate, its endTime has already been set
		if pMetricInfo.IsSucceeded {
			klog.Infof("[OnDeletePod] Pod %s/%s já foi marcado como 'Succeeded'. Nenhuma ação necessária.", p.Namespace, p.Name)
			return pMetricInfo
		}

		// pod is being deleted at this moment
		pMetricInfo.EndTime = time.Now()
		klog.Infof("[OnDeletePod] EndTime atualizado para o pod %s/%s: %v", p.Namespace, p.Name, pMetricInfo.EndTime)
		return pMetricInfo
	})

	klog.Infof("[OnDeletePod] Finalizando a execução do OnDeletePod para o pod %s/%s", p.Namespace, p.Name)
}

func (scheduler *QosDrivenScheduler) UpdatePodMetricInfo(pod *corev1.Pod, f func(PodMetricInfo) PodMetricInfo) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if pod.Namespace == "kube-system" {
		klog.Infof("Pod %s is from kube-system namespace. There is no need to update podMetricInfo.", pod.Name)
		return
	}

	controllerName := ControllerName(pod)
	podName := PodName(pod)

	if scheduler.Controllers == nil {
		scheduler.Controllers = map[string]ControllerMetricInfo{}
	}

	//	klog.Infof("[UpdatePodMetricInfo] controllerName = %s, podName = %s", controllerName, podName)
	cMetricInfo, found := scheduler.Controllers[controllerName]
	//	klog.Infof("[UpdatePodMetricInfo] Controller encontrado? %t", found)

	if !found {
		cMetricInfo.SafetyMargin = scheduler.Args.SafetyMargin.Duration
		cMetricInfo.MinimumRunningTime = scheduler.Args.MinimumRunningTime.Duration
		cMetricInfo.QoSMeasuringApproach = ControllerQoSMeasuring(pod)

		// TODO remove code, it is only for debugging
		kubeclient := scheduler.fh.ClientSet()

		namespace := "default"

		var numberOfReplicas int32
		var expectedPodCompletions int32

		if controllerRef := metav1.GetControllerOf(pod); controllerRef == nil {
			klog.V(1).Infof("pod %s has no controllerRef", pod.Name)
			numberOfReplicas = 1
			expectedPodCompletions = 0
		} else {

			if controllerRef.Kind == "Job" {
				klog.V(1).Infof("Controller is a JOB! ControllerRefName of pod %s is %s ", pod.Name, controllerRef.Name)
				controller, err := kubeclient.BatchV1().Jobs(namespace).Get(context.TODO(), controllerRef.Name, metav1.GetOptions{})

				if err != nil {
					klog.V(1).Infof("ERROR while getting info about Job %v", err)
				} else {
					numberOfReplicas = *controller.Spec.Parallelism
					expectedPodCompletions = *controller.Spec.Completions

					klog.V(1).Infof("The pod %s is associated with Job %s and its parallelism is %v", pod.Name, controllerRef.Name, numberOfReplicas)
				}

			} else {
				klog.V(1).Infof("Controller is NOT a JOB! ControllerRefName of pod %s is %s ", pod.Name, controllerRef.Name)
				controller, err := kubeclient.AppsV1().ReplicaSets(namespace).Get(context.TODO(), controllerRef.Name, metav1.GetOptions{})

				if err != nil {
					klog.V(1).Infof("ERROR while getting info about ReplicaSet %v", err)
				} else {
					numberOfReplicas = *controller.Spec.Replicas
					expectedPodCompletions = 0
					klog.V(1).Infof("The pod %s is associated with ReplicaSet %s and its number of replicas is %v", pod.Name, controllerRef.Name, numberOfReplicas)
				}
			}
		}
		cMetricInfo.NumberOfReplicas = numberOfReplicas
		cMetricInfo.ExpectedPodCompletions = expectedPodCompletions
	}

	// initializing replicas data structure if the cMetricInfo is a new one
	if cMetricInfo.replicas == nil {
		for n := int32(0); n < cMetricInfo.NumberOfReplicas; n++ {
			cMetricInfo.replicas = append(cMetricInfo.replicas, &Replica{
				Incarnations:       map[string]PodMetricInfo{},
				CurrentIncarnation: "",
			})
		}
	}

	oldPodMetricInfo, isNewPod := cMetricInfo.GetPodMetricInfo(pod)
	newPodMetricInfo := f(oldPodMetricInfo)

	// Check if this pod is being deleted, if yes, it needs to realease the replicaId of its controller
	wasMarkedForDeletionNow := false

	if (oldPodMetricInfo.LastStatus != nil) && (newPodMetricInfo.LastStatus != nil) {
		wasMarkedForDeletionNow = (oldPodMetricInfo.LastStatus.DeletionTimestamp == nil) && (newPodMetricInfo.LastStatus.DeletionTimestamp != nil)
	}

	notMarkedForDeletion := (newPodMetricInfo.LastStatus == nil) || (newPodMetricInfo.LastStatus.DeletionTimestamp == nil)
	wasDeletedFromCacheNow := oldPodMetricInfo.EndTime.IsZero() && !newPodMetricInfo.EndTime.IsZero()

	// When the pod is marked for deletion (terminating) we should deallocate it's replicaId.
	// But we can miss this state. In this case, we deallocate when the informer informs pod's deletion
	deallocate := !newPodMetricInfo.IsSucceeded && (wasMarkedForDeletionNow || (notMarkedForDeletion && wasDeletedFromCacheNow))

	// Check if this pod is a new succeeded pod. Is yes, it needs to release the replicaId and increment the SucceededPods variable
	if (oldPodMetricInfo.LastStatus == nil || oldPodMetricInfo.LastStatus.Status.Phase != corev1.PodSucceeded) &&
		((newPodMetricInfo.LastStatus != nil) && (newPodMetricInfo.LastStatus.Status.Phase == corev1.PodSucceeded)) {
		klog.V(1).Infof("The pod %s is being successfully completed", PodName(pod))
		cMetricInfo.SucceededPods++
		newPodMetricInfo.IsSucceeded = true
		newPodMetricInfo.EndTime = time.Now()
		deallocate = true
	}

	// If the pod was deleted or succeeded, dump it and disassociate it from the replica's currentIncarnation
	if deallocate {
		cMetricInfoClone := cMetricInfo.Clone()
		cMetricInfo.DeallocateReplicaId(pod)

		cMetricInfoClone.ReferencePod = pod
		go dumpMetrics(cMetricInfoClone, newPodMetricInfo, scheduler.lock.RLocker())
	}

	// This is a new pod and it needs to be associated with a replicaId of the controller
	if isNewPod {
		replicaId := cMetricInfo.AllocateReplicaId(pod)
		newPodMetricInfo.ReplicaId = replicaId
		klog.V(1).Infof("the pod %s was new, allocated replicaId = %d", PodName(pod), replicaId)
	}

	previouslyRunningPods := cMetricInfo.NumberOfRunningPods()
	replicaId, err := cMetricInfo.GetPodReplicaId(pod)
	if err != nil {
		klog.V(1).Infof("if the pod was new, it should have been allocated (see variable isNewPod)")
		panic(err)
	}

	cMetricInfo.replicas[replicaId].Incarnations[podName] = newPodMetricInfo
	scheduler.Controllers[controllerName] = cMetricInfo
	klog.V(1).Infof("The number of pods running of %s was %d and now is %d", controllerName, previouslyRunningPods, cMetricInfo.NumberOfRunningPods())
	klog.V(1).Infof("The number of succeeded pods of %s is %d", controllerName, cMetricInfo.SucceededPods)
}

func (scheduler *QosDrivenScheduler) addEventHandler() *QosDrivenScheduler {
	scheduler.PodInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		// When a new pod gets created
		AddFunc: scheduler.OnAddPod,
		// When a pod gets updated
		UpdateFunc: scheduler.OnUpdatePod,
		// When a pod gets deleted
		DeleteFunc: scheduler.OnDeletePod,
	})
	return scheduler
}

func dumpMetrics(cMetricInfo ControllerMetricInfo, pMetricInfo PodMetricInfo, accessReaderLocker sync.Locker) {
	podName := PodName(pMetricInfo.LastStatus)
	controllerName := ControllerName(pMetricInfo.LastStatus)
	klog.V(1).Infof("Dumping metrics of pod %s (status %s | controller %s)", podName, pMetricInfo.LastStatus.Status.Phase, controllerName)

	success := false
	timeRef := time.Now()
	cMetrics := cMetricInfo.Metrics(timeRef, accessReaderLocker)

	// timestamp, podName, controllerName, replicaId, qosMeasuring, waitingTime, allocationTime, runningTime, terminationStatus, controllerQoS, qosMetric
	entry := fmt.Sprintf("%d,%s,%s,%f,%d,%s,%d,%d,%d,%d,%t,%f,%f,%d,%d,%d,%d\n",
		time.Now().Unix(),
		podName,
		controllerName,
		ControllerSlo(pMetricInfo.LastStatus),
		pMetricInfo.ReplicaId,
		cMetrics.QoSMeasuringApproach,
		pMetricInfo.CreationTime.Unix(),
		pMetricInfo.Waiting(timeRef).Milliseconds(),
		pMetricInfo.Binding(timeRef).Milliseconds(),
		pMetricInfo.Running(timeRef).Milliseconds(),
		pMetricInfo.IsSucceeded,
		cMetrics.QoS(),
		cMetrics.QoSMetric(pMetricInfo.LastStatus),
		cMetrics.WaitingTime.Milliseconds(),
		cMetrics.BindingTime.Milliseconds(),
		cMetrics.EffectiveRunningTime.Milliseconds(),
		cMetrics.DiscardedRunningTime.Milliseconds())

	for !success {
		f, err := os.OpenFile("/metrics.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err == nil {
			_, err = f.WriteString(entry)

			if err == nil {
				success = true
			}
		}

		if !success {
			klog.V(1).Infof("Failed to log pod metrics: %s, retrying...\n", err)
			time.Sleep(time.Second)
		}
	}
}

type QosDrivenSchedulerArgs struct {
	// Controllers with a time-to-violate below this configured SafetyMargin
	// is treated as close to violate it's SLO by our scheduler.
	SafetyMargin metav1.Duration `json:"safetyMargin"`
	// AcceptablePreemptionOverhead is a global configuration to provide a default
	// acceptable preemption overhead to controllers without explicit one.
	AcceptablePreemptionOverhead float64 `json:"acceptablePreemptionOverhead,omitempty"`
	// Pods will run MinimumRunningTime until it can be preempted by another pod with same importance.
	MinimumRunningTime metav1.Duration `json:"minimumRunningTime"`
}

func format(d time.Duration) string {
	if d > time.Second {
		return d.Truncate(time.Second).String()
	}

	return d.Truncate(time.Millisecond).String()
}

func sortedKeys(m map[string]ControllerMetricInfo) []string {
	keys := make([]string, len(m))
	idx := 0
	for key := range m {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return keys
}

func (scheduler *QosDrivenScheduler) list(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprintf(w, "%20s  %9s  %19s  %15s  %15s  %15s  %20s  %20s  %20s  %20s\n",
		"CONTROLLER",
		"QOS/SLO",
		"PREEMPTION OVERHEAD",
		"QOS APPROACH",
		"POD PHASE",
		"QOS METRIC",
		"EFF RUNNING",
		"DIS RUNNING",
		"WAITING",
		"BINDING")

	scheduler.lock.RLock()
	defer scheduler.lock.RUnlock()

	now := time.Now()
	cNames := sortedKeys(scheduler.Controllers)

	for _, cName := range cNames {
		controller := scheduler.Controllers[cName]

		for replicaId, replica := range controller.replicas {
			pod := replica.Incarnations[replica.CurrentIncarnation].LastStatus

			// listing only pods that are active in the system
			if pod != nil {
				// FIXME: scheduler.GetControllerMetricInfo also locks and this lock may cause deadlock when recursive locking
				cMetricInfo := scheduler.GetControllerMetricInfo(pod)
				metrics := cMetricInfo.Metrics(now, noopLocker)
				fmt.Fprintf(w, "%20s  %4.2f/%4.2f  %9.2f/%-9.2f  %15s  %15s  %15.4f  %20s  %20s  %20s  %20s\n",
					cName+"-"+strconv.Itoa(replicaId),
					metrics.QoS(),
					ControllerSlo(pod),
					metrics.PreemptionOverhead(),
					scheduler.AcceptablePreemptionOverhead(pod),
					cMetricInfo.QoSMeasuringApproach,
					pod.Status.Phase,
					metrics.QoSMetric(pod),
					format(metrics.EffectiveRunningTime),
					format(metrics.DiscardedRunningTime),
					format(metrics.WaitingTime),
					format(metrics.BindingTime))
			}
		}
	}
}

func (scheduler *QosDrivenScheduler) debugApi() {
	http.HandleFunc("/", scheduler.list)
	klog.Fatal(http.ListenAndServe(":10000", nil))
}

func (scheduler *QosDrivenScheduler) GetControllerMetricInfo(pod *corev1.Pod) ControllerMetricInfo {
	klog.Infof("[GetControllerMetricInfo] Iniciando para o pod: %s/%s", pod.Namespace, pod.Name)

	scheduler.lock.RLock()
	defer scheduler.lock.RUnlock()

	klog.Infof("[GetControllerMetricInfo] Obtendo o nome do controlador para o pod: %s/%s", pod.Namespace, pod.Name)
	cMetricInfo := scheduler.Controllers[ControllerName(pod)]
	cMetricInfo.ReferencePod = pod
	klog.Infof("[GetControllerMetricInfo] Métricas do controlador definidas para o pod: %s/%s", pod.Namespace, pod.Name)

	return cMetricInfo
}

// getUpdatedVersion returns the latest pod object in cache
func (scheduler *QosDrivenScheduler) getUpdatedVersion(pod *corev1.Pod) PodMetricInfo {
	scheduler.lock.RLock()
	defer scheduler.lock.RUnlock()

	cMetricInfo := scheduler.Controllers[ControllerName(pod)]
	pMetricInfo, _ := cMetricInfo.GetPodMetricInfo(pod)

	return pMetricInfo
}

func NewQosScheduler(fh framework.Handle) *QosDrivenScheduler {
	podInformer := fh.SharedInformerFactory().Core().V1().Pods()

	return (&QosDrivenScheduler{
		PodInformer: podInformer.Informer(),
		PodLister:   podInformer.Lister(),
		fh:          fh,
		Controllers: map[string]ControllerMetricInfo{},
	}).addEventHandler()
}

// Função de inicialização do plugin
func New() func(ctx context.Context, args runtime.Object, f framework.Handle) (framework.Plugin, error) {
	return func(ctx context.Context, args runtime.Object, f framework.Handle) (framework.Plugin, error) {
		// Inicializa o scheduler com o handle fornecido
		scheduler := NewQosScheduler(f)

		// Define valores padrão para os argumentos
		scheduler.Args.AcceptablePreemptionOverhead = DefaultAcceptablePreemptionOverhead

		// Decodifica os argumentos fornecidos pelo usuário (se existirem)
		if err := frameworkruntime.DecodeInto(args, &scheduler.Args); err != nil {
			return nil, err
		}

		// Inicia o SharedInformerFactory
		f.SharedInformerFactory().Start(ctx.Done())
		f.SharedInformerFactory().WaitForCacheSync(ctx.Done())

		if !cache.WaitForCacheSync(ctx.Done(), scheduler.PodInformer.HasSynced) {
			klog.Fatalf("Falha ao sincronizar o cache do PodInformer")
		} else {
			klog.Infof("PodInformer sincronizado com sucesso")
		}

		// Loga os argumentos recebidos para depuração
		klog.V(1).Infof("Plugin iniciado com argumentos: %+v", scheduler.Args)

		// Inicia a API de depuração, se necessário
		go scheduler.debugApi()

		// Retorna o plugin inicializado
		return scheduler, nil
	}
}
