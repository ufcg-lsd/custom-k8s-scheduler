package qos_driven_scheduler

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name                                = "QosAware"
	DefaultSlo                          = 0.80
	DefaultAcceptablePreemptionOverhead = 1.00
)

// QosDrivenScheduler implements the requested plugin interfaces
type QosDrivenScheduler struct {
	fh                    framework.Handle
	args                  QosDrivenSchedulerArgs
	PodInformer           cache.SharedIndexInformer
	Controllers           map[string]ControllerMetricInfo
	lock                  sync.RWMutex
	podLister             corelisters.PodLister
	pdbLister             policylisters.PodDisruptionBudgetLister
	Evaluator             *Evaluator
	enableAsyncPreemption bool
}

// Ensuring that QosDrivenScheduler implements the required interfaces
var _ framework.QueueSortPlugin = &QosDrivenScheduler{}
var _ framework.ReservePlugin = &QosDrivenScheduler{}
var _ framework.PreBindPlugin = &QosDrivenScheduler{}
var _ framework.PostBindPlugin = &QosDrivenScheduler{}
var _ framework.PostFilterPlugin = &QosDrivenScheduler{}
var _ framework.PreEnqueuePlugin = &QosDrivenScheduler{}

// Name returns the name of the plugin
func (scheduler *QosDrivenScheduler) Name() string {
	return Name
}

// QueueSortPlugin: Determines the priority order of pods in the queue
func (scheduler *QosDrivenScheduler) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
//	klog.V(2).Infof("[QueueSort] Comparing priority of %s with %s", pInfo1.Pod.Name, pInfo2.Pod.Name)
	if pInfo1.Pod.Name == pInfo2.Pod.Name && pInfo1.Pod.Namespace == pInfo2.Pod.Namespace {
//		klog.Warningf("[QueueSort] Attempt to compare pod %s with itself. Ignoring comparison.", pInfo1.Pod.Name)
		return false
	}
	precedence := scheduler.HigherPrecedence(pInfo1.Pod, pInfo2.Pod)
//	klog.V(2).Infof("[QueueSort] Precedence determined: Does %s have precedence over %s? %v", pInfo1.Pod.Name, pInfo2.Pod.Name, precedence)
	return precedence
}

// We calculate precedence between pods checking if the time to violate p1's controller SLO is lower than p2's controller.
// If the pods' controllers has different importances and their time to violate are below the safety margin the precedence is for the highest importance controller's pod.
func (scheduler *QosDrivenScheduler) HigherPrecedence(p1, p2 *corev1.Pod) bool {
	if p1.Name == p2.Name && p1.Namespace == p2.Namespace {
		return false
	}

	now := time.Now()
//	klog.V(2).Infof("[HigherPrecedence] Calculating precedence between %s and %s", p1.Name, p2.Name)

	scheduler.WaitForPodsOnCache(p1, p2)
	cMetricInfo1 := scheduler.GetControllerMetricInfo(p1)
	cMetrics1 := cMetricInfo1.Metrics(now, scheduler.lock.RLocker())
	cMetricInfo2 := scheduler.GetControllerMetricInfo(p2)
	cMetrics2 := cMetricInfo2.Metrics(now, scheduler.lock.RLocker())

	importance1 := ControllerImportance(p1)
	importance2 := ControllerImportance(p2)

	qosMetric1 := cMetrics1.QoSMetric(p1)
	qosMetric2 := cMetrics2.QoSMetric(p2)

//	klog.V(2).Infof("[HigherPrecedence] Metrics for %s: QoS = %f, Importance = %.2f", p1.Name, qosMetric1, importance1)
//	klog.V(2).Infof("[HigherPrecedence] Metrics for %s: QoS = %f, Importance = %.2f", p2.Name, qosMetric2, importance2)

	safetyMargin := scheduler.args.SafetyMargin.Duration.Seconds()

	// Is in resource contention
	if (qosMetric1 < safetyMargin) && (qosMetric2 < safetyMargin) && importance1 != importance2 {
//		klog.V(2).Infof("[HigherPrecedence] Both are in resource contention and have different importances.")
		return importance1 > importance2
	}

	result := qosMetric1 < qosMetric2
//	klog.V(2).Infof("[HigherPrecedence] Final decision: Does %s have precedence over %s? %v", p1.Name, p2.Name, result)
	return result
}

// TODO maybe we should sort only by controller's SLO and importance if we can't find its metrics
func (scheduler *QosDrivenScheduler) WaitForPodsOnCache(pods ...*corev1.Pod) {
	for _, pod := range pods {
//		klog.V(2).Infof("[WaitForPodsOnCache] Checking if pod %s is in the cache...", pod.Name)
		cacheMiss := func() bool {
			cMetricInfo := scheduler.GetControllerMetricInfo(pod)
			scheduler.lock.RLock()
			defer scheduler.lock.RUnlock()

			_, notFound := cMetricInfo.GetPodMetricInfo(pod)
			if notFound {
				klog.Warningf("[cacheMiss] Metrics not found for pod: %s/%s", pod.Namespace, pod.Name)
			}

			return notFound
		}

		for cacheMiss() {
			klog.Warningf("[WaitForPodsOnCache] Pod %s not found in cache. Retrying...", pod.Name)
			time.Sleep(time.Millisecond * 100)
		}

//		klog.V(2).Infof("[WaitForPodsOnCache] Pod %s found in cache.", pod.Name)
	}
}

// ReservePlugin: Called when resources are reserved
func (scheduler *QosDrivenScheduler) Reserve(_ context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
//	now := time.Now()
//	klog.V(2).Infof("[Reserve] resources reserved at node %s for pod %s at %s", nodeName, PodName(pod), now)
	scheduler.UpdatePodMetricInfo(pod, func(old PodMetricInfo) PodMetricInfo {
//		klog.V(2).Infof("[Reserve] Current allocation status for pod %s: %v", PodName(pod), old.AllocationStatus)
		old.AllocationStatus = AllocatingState
//		klog.V(2).Infof("[Reserve] Allocation status updated for pod %s: %v", PodName(pod), AllocatingState)
		return old
	})
	return nil
}

// ReservePlugin: Called when the reservation is undone
func (scheduler *QosDrivenScheduler) Unreserve(_ context.Context, _ *framework.CycleState, pod *corev1.Pod, nodeName string) {
//	now := time.Now()
//	klog.V(2).Infof("[Unreserve] Resources are being released on node %s for pod %s at %s", nodeName, PodName(pod), now)

//	klog.V(2).Infof("[Unreserve] Updating metrics for pod %s to release resources", PodName(pod))
	scheduler.UpdatePodMetricInfo(pod, func(old PodMetricInfo) PodMetricInfo {
//		klog.V(2).Infof("[Unreserve] Previous AllocationStatus for pod %s: %v", PodName(pod), old.AllocationStatus)
		old.AllocationStatus = ""
//		klog.V(2).Infof("[Unreserve] New AllocationStatus for pod %s: %v", PodName(pod), old.AllocationStatus)
		return old
	})

//	klog.V(2).Infof("[Unreserve] Resources successfully released for pod %s on node %s", PodName(pod), nodeName)
}

// PreBindPlugin: Called before the pod is bound to the node
func (scheduler *QosDrivenScheduler) PreBind(_ context.Context, state *framework.CycleState, p *corev1.Pod, nodeName string) *framework.Status {
//	klog.V(2).Infof("[PreBind] Validating pod %s before binding to node %s", p.Name, nodeName)
	now := time.Now()
	state.Write(BindingStart, CloneableTime{now})
	return nil
}

// PostBindPlugin: Called after the pod is bound to the node
func (scheduler *QosDrivenScheduler) PostBind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, _ string) {
//	klog.V(2).Infof("[PostBind] Called for pod %s", pod.Name)

	// Attempt to read the `BindingStart` state
	start, err := state.Read(BindingStart)
	if err != nil {
		klog.Errorf("[PostBind] Error reading BindingStart for pod %s: %v", pod.Name, err)
		return
	}

	// Check the type of the returned value
	startBinding, ok := start.(CloneableTime)
	if !ok {
		klog.Errorf("[PostBind] Unexpected type for BindingStart in pod %s", pod.Name)
		return
	}

	// Calculate the binding time
	endBinding := time.Now()
//	klog.V(2).Infof("[PostBind] Pod %s - Binding start: %s, Binding end: %s", pod.Name, startBinding.Time, endBinding)

	// Update the pod metrics
	scheduler.UpdatePodMetricInfo(pod, func(old PodMetricInfo) PodMetricInfo {
		old.StartBindingTime = startBinding.Time
		old.StartRunningTime = endBinding
		old.AllocationStatus = AllocatedState
//		klog.V(2).Infof("[PostBind] Metrics updated for pod %s", pod.Name)
		return old
	})
}

func (scheduler *QosDrivenScheduler) OnAddPod(obj interface{}) {

	p := obj.(*corev1.Pod).DeepCopy()
//	klog.V(2).Infof("[OnAddPod] Pod added: %s/%s, Annotations: %+v", p.Namespace, p.Name, p.Annotations)

	if p.Namespace != "default" {
//		klog.V(2).Infof("[OnAddPod] Ignoring pod %s/%s as it belongs to the kube-system namespace", p.Namespace, p.Name)
		return
	}

	start := p.GetCreationTimestamp().Time
//	klog.V(2).Infof("[OnAddPod] Adding pod %s/%s with the following details:\n%s", p.Namespace, p.Name, p.String())

	// CreationTimestamp is marked as +optional in the API, so we put this fallback
	if start.IsZero() {
		start = time.Now()
//		klog.V(2).Infof("[OnAddPod] Creation timestamp not set for pod %s/%s, using current time: %v", p.Namespace, p.Name, start)
	}

	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
//		klog.V(2).Infof("[OnAddPod] Updating metrics for pod %s/%s", p.Namespace, p.Name)
		pMetricInfo.ControllerName = ControllerName(p)
		pMetricInfo.LastStatus = p
		pMetricInfo.CreationTime = start

		// pod starts running at this moment
		if pMetricInfo.StartRunningTime.IsZero() && p.Status.Phase == corev1.PodRunning {
			pMetricInfo.StartRunningTime = time.Now()
//			klog.V(2).Infof("[OnAddPod] Pod %s/%s entered Running state", p.Namespace, p.Name)
		}

		return pMetricInfo
	})
//	klog.V(2).Infof("[OnAddPod] Pod %s/%s successfully added", p.Namespace, p.Name)
}

func (scheduler *QosDrivenScheduler) OnUpdatePod(_, newObj interface{}) {

//	klog.V(2).Infof("[OnUpdatePod] Starting pod update processing")

	// Creating a copy of the updated pod
	p := newObj.(*corev1.Pod).DeepCopy()
//	klog.V(2).Infof("[OnUpdatePod] Updated pod detected: %s/%s, Annotations: %+v", p.Namespace, p.Name, p.Annotations)

	// Ignoring pods in the kube-system namespace
	if p.Namespace != "default" {
//		klog.V(2).Infof("[OnUpdatePod] Pod %s/%s belongs to the 'kube-system' namespace. Ignoring...", p.Namespace, p.Name)
		return
	}

	// Updating pod metric information
//	klog.V(2).Infof("[OnUpdatePod] Updating metric information for pod %s/%s", p.Namespace, p.Name)
	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
//		klog.V(2).Infof("[OnUpdatePod] Updating 'LastStatus' of pod %s/%s in metrics", p.Namespace, p.Name)
		pMetricInfo.LastStatus = p
//		klog.V(2).Infof("[OnUpdatePod] Metric updated for pod %s/%s", p.Namespace, p.Name)
		return pMetricInfo
	})

//	klog.V(2).Infof("[OnUpdatePod] Finished pod update processing for %s/%s", p.Namespace, p.Name)
}

func (scheduler *QosDrivenScheduler) OnDeletePod(lastState interface{}) {

//	klog.V(2).Infof("[OnDeletePod] Starting execution of OnDeletePod")

	p, ok := lastState.(*corev1.Pod)
	if !ok {
		klog.Warningf("[OnDeletePod] The previous state of the deleted pod is unknown")
		return
	}

	p = p.DeepCopy()
//	klog.V(2).Infof("[OnDeletePod] DeepCopy performed for pod %s/%s", p.Namespace, p.Name)

	if p.Namespace != "default" {
//		klog.V(2).Infof("[OnDeletePod] Pod %s/%s belongs to the 'kube-system' namespace. Ignoring...", p.Namespace, p.Name)
		return
	}

//	klog.V(2).Infof("[OnDeletePod] Deleted pod detected: %s/%s\nPod details: %s", p.Namespace, p.Name, p.String())

//	klog.V(2).Infof("[OnDeletePod] Updating metrics for pod %s/%s", p.Namespace, p.Name)
	scheduler.UpdatePodMetricInfo(p, func(pMetricInfo PodMetricInfo) PodMetricInfo {
//		klog.V(2).Infof("[OnDeletePod] Checking the state of pod %s/%s in metrics", p.Namespace, p.Name)

		// if pod was succeeded terminate, its endTime has already been set
		if pMetricInfo.IsSucceeded {
//			klog.V(2).Infof("[OnDeletePod] Pod %s/%s has already been marked as 'Succeeded'. No action needed.", p.Namespace, p.Name)
			return pMetricInfo
		}

		// pod is being deleted at this moment
		pMetricInfo.EndTime = time.Now()
//		klog.V(2).Infof("[OnDeletePod] EndTime updated for pod %s/%s: %v", p.Namespace, p.Name, pMetricInfo.EndTime)
		return pMetricInfo
	})

//	klog.V(2).Infof("[OnDeletePod] Finishing execution of OnDeletePod for pod %s/%s", p.Namespace, p.Name)
}

func (scheduler *QosDrivenScheduler) UpdatePodMetricInfo(pod *corev1.Pod, f func(PodMetricInfo) PodMetricInfo) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if pod.Namespace == "kube-system" {
//		klog.V(2).Infof("Pod %s is from kube-system namespace. There is no need to update podMetricInfo.", pod.Name)
		return
	}

	controllerName := ControllerName(pod)
	podName := PodName(pod)

	if scheduler.Controllers == nil {
		scheduler.Controllers = map[string]ControllerMetricInfo{}
	}

	//	klog.V(2).Infof("[UpdatePodMetricInfo] controllerName = %s, podName = %s", controllerName, podName)
	cMetricInfo, found := scheduler.Controllers[controllerName]
	//	klog.V(2).Infof("[UpdatePodMetricInfo] Controller encontrado? %t", found)

	if !found {
		cMetricInfo.SafetyMargin = scheduler.args.SafetyMargin.Duration
		cMetricInfo.MinimumRunningTime = scheduler.args.MinimumRunningTime.Duration
		cMetricInfo.QoSMeasuringApproach = ControllerQoSMeasuring(pod)

		// TODO remove code, it is only for debugging
		kubeclient := scheduler.fh.ClientSet()

		namespace := "default"

		var numberOfReplicas int32
		var expectedPodCompletions int32

		if controllerRef := metav1.GetControllerOf(pod); controllerRef == nil {
//			klog.V(1).Infof("pod %s has no controllerRef", pod.Name)
			numberOfReplicas = 1
			expectedPodCompletions = 0
		} else {

			if controllerRef.Kind == "Job" {
//				klog.V(1).Infof("Controller is a JOB! ControllerRefName of pod %s is %s ", pod.Name, controllerRef.Name)
				controller, err := kubeclient.BatchV1().Jobs(namespace).Get(context.TODO(), controllerRef.Name, metav1.GetOptions{})

				if err != nil {
//					klog.V(1).Infof("ERROR while getting info about Job %v", err)
				} else {
					numberOfReplicas = *controller.Spec.Parallelism
					expectedPodCompletions = *controller.Spec.Completions

//					klog.V(1).Infof("The pod %s is associated with Job %s and its parallelism is %v", pod.Name, controllerRef.Name, numberOfReplicas)
				}

			} else {
//				klog.V(1).Infof("Controller is NOT a JOB! ControllerRefName of pod %s is %s ", pod.Name, controllerRef.Name)
				controller, err := kubeclient.AppsV1().ReplicaSets(namespace).Get(context.TODO(), controllerRef.Name, metav1.GetOptions{})

				if err != nil {
//					klog.V(1).Infof("ERROR while getting info about ReplicaSet %v", err)
				} else {
					numberOfReplicas = *controller.Spec.Replicas
					expectedPodCompletions = 0
//					klog.V(1).Infof("The pod %s is associated with ReplicaSet %s and its number of replicas is %v", pod.Name, controllerRef.Name, numberOfReplicas)
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
//		klog.V(1).Infof("The pod %s is being successfully completed", PodName(pod))
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
//		klog.V(1).Infof("the pod %s was new, allocated replicaId = %d", PodName(pod), replicaId)
	}

//	previouslyRunningPods := cMetricInfo.NumberOfRunningPods()
	replicaId, err := cMetricInfo.GetPodReplicaId(pod)
	if err != nil {
//		klog.V(1).Infof("if the pod was new, it should have been allocated (see variable isNewPod)")
		panic(err)
	}

	cMetricInfo.replicas[replicaId].Incarnations[podName] = newPodMetricInfo
	scheduler.Controllers[controllerName] = cMetricInfo
//	klog.V(1).Infof("The number of pods running of %s was %d and now is %d", controllerName, previouslyRunningPods, cMetricInfo.NumberOfRunningPods())
//	klog.V(1).Infof("The number of succeeded pods of %s is %d", controllerName, cMetricInfo.SucceededPods)
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
//	klog.V(1).Infof("Dumping metrics of pod %s (status %s | controller %s)", podName, pMetricInfo.LastStatus.Status.Phase, controllerName)

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
//			klog.V(1).Infof("Failed to log pod metrics: %s, retrying...\n", err)
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

	metav1.TypeMeta

	// MinCandidateNodesPercentage is the minimum number of candidates to
	// shortlist when dry running preemption as a percentage of number of nodes.
	// Must be in the range [0, 100]. Defaults to 10% of the cluster size if
	// unspecified.
	MinCandidateNodesPercentage int32
	// MinCandidateNodesAbsolute is the absolute minimum number of candidates to
	// shortlist. The likely number of candidates enumerated for dry running
	// preemption is given by the formula:
	// numCandidates = max(numNodes * minCandidateNodesPercentage, minCandidateNodesAbsolute)
	// We say "likely" because there are other factors such as PDB violations
	// that play a role in the number of candidates shortlisted. Must be at least
	// 0 nodes. Defaults to 100 nodes if unspecified.
	MinCandidateNodesAbsolute int32
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
//	klog.V(2).Infof("[GetControllerMetricInfo] Starting for pod: %s/%s", pod.Namespace, pod.Name)

	scheduler.lock.RLock()
	defer scheduler.lock.RUnlock()

//	klog.V(2).Infof("[GetControllerMetricInfo] Getting controller name for pod: %s/%s", pod.Namespace, pod.Name)
	cMetricInfo := scheduler.Controllers[ControllerName(pod)]
	cMetricInfo.ReferencePod = pod
//	klog.V(2).Infof("[GetControllerMetricInfo] Controller metrics set for pod: %s/%s", pod.Namespace, pod.Name)

	return cMetricInfo
}

// isPodContributingToImproveControllerQoS receives a pod and return false if it doesn't contribute to increase
// the QoS of its controller, this happen only when the pod is associated with a controller that use
// the concurrent qos measuring approach and at least one pod from the same controller is not running
//
// this function assumes that the received pod is running.
func (scheduler *QosDrivenScheduler) isPodContributingToImproveControllerQoS(pod *corev1.Pod) bool {
	// These ones are critical pods and they always be considered as very important.
	if pod.Namespace == "kube-system" {
		return true
	}

	cMetricInfo := scheduler.GetControllerMetricInfo(pod)

	if cMetricInfo.QoSMeasuringApproach == ConcurrentQosMeasuring {
		scheduler.lock.RLock()
		defer scheduler.lock.RUnlock()
//		klog.V(1).Infof("[CHECKING POD CONTRIBUTES TO QOS]: concurrent controller %s | replication %d | running_pods %d | allocating_pods %d", ControllerName(pod),
//			cMetricInfo.NumberOfReplicas, cMetricInfo.NumberOfRunningPods(), cMetricInfo.NumberOfPodsBeingAllocated())
		return cMetricInfo.NumberOfRunningPods()+cMetricInfo.NumberOfPodsBeingAllocated() == cMetricInfo.NumberOfReplicas
	}
	return true
}

func (scheduler *QosDrivenScheduler) filterNodesByHighestPreemptionScore(
	candidateNodes []string,
	podClass float64,
	nodeToPreemptionScore map[string]map[float64]float64,
) []string {

	var newCandidateNodes []string
	var maxPreemptionClassScore = float64(-1.0 * math.MaxInt32)
	for _, node := range candidateNodes {
		preemptionClassScore := nodeToPreemptionScore[node][podClass]

		if preemptionClassScore > maxPreemptionClassScore {
			maxPreemptionClassScore = preemptionClassScore
			newCandidateNodes = nil
		}
		if preemptionClassScore == maxPreemptionClassScore {
			newCandidateNodes = append(newCandidateNodes, node)
		}
	}

	return newCandidateNodes
}


func New() func(ctx context.Context, args runtime.Object, f framework.Handle) (framework.Plugin, error) {
	return func(ctx context.Context, args runtime.Object, f framework.Handle) (framework.Plugin, error) {
		
		unstructuredArgs, err := runtime.DefaultUnstructuredConverter.ToUnstructured(args)
		if err != nil {
			return nil, fmt.Errorf("failed to convert args to unstructured format: %v", err)
		}

		var schedulerArgs QosDrivenSchedulerArgs
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredArgs, &schedulerArgs); err != nil {
			return nil, fmt.Errorf("failed to convert unstructured args to QosDrivenSchedulerArgs: %v", err)
		}

		podInformer := f.SharedInformerFactory().Core().V1().Pods()
		podLister := f.SharedInformerFactory().Core().V1().Pods().Lister()
		pdbLister := getPDBLister(f.SharedInformerFactory())

		// Determine if asynchronous preemption is enabled
		enableAsyncPreemption := false

		// Initialize the scheduler with the converted arguments
		scheduler := QosDrivenScheduler{
			PodInformer:           podInformer.Informer(),
			fh:                    f,
			args:                  schedulerArgs,
			podLister:             podLister,
			pdbLister:             pdbLister,
			Controllers:           map[string]ControllerMetricInfo{},
			enableAsyncPreemption: enableAsyncPreemption,
		}

		scheduler.addEventHandler()

		// Initialize preemption logic
		scheduler.Evaluator = NewEvaluator(Name, f, &scheduler, scheduler.enableAsyncPreemption)

		// Start the debug API if necessary
		go scheduler.debugApi()

		// Returns the initialized plugin
		return &scheduler, nil
	}
}

func getPDBLister(informerFactory informers.SharedInformerFactory) policylisters.PodDisruptionBudgetLister {
	return informerFactory.Policy().V1().PodDisruptionBudgets().Lister()
}
