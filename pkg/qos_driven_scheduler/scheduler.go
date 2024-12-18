package qos_driven_scheduler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name                                = "QosAware"
	DefaultSlo                          = 0.80
	DefaultAcceptablePreemptionOverhead = 1.00
)

// QosDrivenScheduler implementa as interfaces de plugins solicitadas
type QosDrivenScheduler struct {
	Args        QosDrivenSchedulerArgs
	PodInformer cache.SharedIndexInformer
	lock        sync.RWMutex
	Controllers map[string]ControllerMetricInfo
	fh          framework.Handle
}

// Garantindo que QosDrivenScheduler implementa as interfaces necessárias
var _ framework.QueueSortPlugin = &QosDrivenScheduler{}
var _ framework.ReservePlugin = &QosDrivenScheduler{}
var _ framework.PreBindPlugin = &QosDrivenScheduler{}
var _ framework.PostBindPlugin = &QosDrivenScheduler{}
var _ framework.PostFilterPlugin = &QosDrivenScheduler{}

// Name retorna o nome do plugin
func (scheduler *QosDrivenScheduler) Name() string {
	return Name
}

// QueueSortPlugin: Determina a ordem de prioridade dos pods na fila
func (scheduler *QosDrivenScheduler) Less(p1, p2 *framework.QueuedPodInfo) bool {
	klog.Infof("[QueueSort] Comparando prioridade de %s com %s", p1.Pod.Name, p2.Pod.Name)
	return p1.Pod.CreationTimestamp.Before(&p2.Pod.CreationTimestamp)
}

// ReservePlugin: Chamado quando os recursos são reservados
func (scheduler *QosDrivenScheduler) Reserve(_ context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	klog.Infof("[Reserve] Reservando recursos para o pod %s no node %s", pod.Name, nodeName)
	return framework.NewStatus(framework.Success)
}

// ReservePlugin: Chamado quando a reserva é desfeita
func (scheduler *QosDrivenScheduler) Unreserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	klog.Infof("[Unreserve] Desfazendo reserva para o pod %s no node %s", pod.Name, nodeName)
}

// PreBindPlugin: Chamado antes de o pod ser vinculado ao node
func (scheduler *QosDrivenScheduler) PreBind(_ context.Context, state *framework.CycleState, p *corev1.Pod, _ string) *framework.Status {
	now := time.Now()
	state.Write(BindingStart, CloneableTime{now})
	return nil
}

// PostBindPlugin: Chamado após o pod ser vinculado ao node
func (scheduler *QosDrivenScheduler) PostBind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	klog.Infof("[PostBind] Pod %s vinculado ao node %s com sucesso", pod.Name, nodeName)
}

// PostFilterPlugin: Chamado quando nenhum node satisfaz os requisitos do pod
func (scheduler *QosDrivenScheduler) PostFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeToStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	klog.Infof("[PostFilter] Avaliando pod %s após falha no filtro", pod.Name)

	// Implementação básica: nenhuma preempção ou recuperação
	return &framework.PostFilterResult{}, framework.NewStatus(framework.Unschedulable)
}

func (scheduler *QosDrivenScheduler) OnAddPod(obj interface{}) {
	p := obj.(*corev1.Pod).DeepCopy()
	if p.Namespace == "kube-system" {
		return
	}

	start := p.GetCreationTimestamp().Time
	klog.V(1).Infof("Pods %s being added:\n%s", PodName(p), p.String())

	// CreationTimestamp is marked as +optional in the API, so we put this fallback
	if start.IsZero() {
		start = time.Now()
	}

	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
		pMetricInfo.ControllerName = ControllerName(p)
		pMetricInfo.LastStatus = p
		pMetricInfo.CreationTime = start

		// pod starts running at this moment
		if pMetricInfo.StartRunningTime.IsZero() && p.Status.Phase == corev1.PodRunning {
			pMetricInfo.StartRunningTime = time.Now()
		}

		return pMetricInfo
	})
	klog.V(1).Infof("Pods %s added successfully", PodName(p))
}

func (scheduler *QosDrivenScheduler) OnUpdatePod(_, newObj interface{}) {
	p := newObj.(*corev1.Pod).DeepCopy()
	if p.Namespace == "kube-system" {
		return
	}
	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
		pMetricInfo.LastStatus = p
		return pMetricInfo
	})
}

func (scheduler *QosDrivenScheduler) OnDeletePod(lastState interface{}) {
	p, ok := lastState.(*corev1.Pod)
	if !ok {
		klog.V(1).Infof("last state from deleted pod was unknown")
		return
	}
	p = p.DeepCopy()
	if p.Namespace == "kube-system" {
		return
	}
	klog.V(1).Infof("Pod %s deleted:\n%s", PodName(p), p.String())

	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
		// if pod was succeeded terminate, its endTime has already been set
		if pMetricInfo.IsSucceeded {
			return pMetricInfo
		}

		// pod is being deleted at this moment
		pMetricInfo.EndTime = time.Now()
		return pMetricInfo
	})
}

func (scheduler *QosDrivenScheduler) UpdatePodMetricInfo(pod *corev1.Pod, f func(PodMetricInfo) PodMetricInfo) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if pod.Namespace == "kube-system" {
		klog.V(1).Infof("Pod %s is from kube-system namespace. There is no need to update podMetricInfo.", pod.Name)
		return
	}

	controllerName := ControllerName(pod)
	podName := PodName(pod)

	if scheduler.Controllers == nil {
		scheduler.Controllers = map[string]ControllerMetricInfo{}
	}

	cMetricInfo, found := scheduler.Controllers[controllerName]
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

// Função de inicialização do plugin
func New() func(ctx context.Context, args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return func(ctx context.Context, args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
		klog.Infof("Plugin %s inicializado com sucesso", Name)
		return &QosDrivenScheduler{}, nil
	}
}
