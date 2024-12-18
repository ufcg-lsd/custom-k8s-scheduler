package qos_driven_scheduler

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// This type should do nothing in any implementation
type noop struct{}

func (n noop) Lock()   {}
func (n noop) Unlock() {}

// This lock does not lock
var noopLocker sync.Locker = noop{}

type Replica struct {
	Incarnations       map[string]PodMetricInfo
	CurrentIncarnation string
}

// If a replica does not have a currentIncarnation, it is not running neither pending
func (replica *Replica) IsRunning() bool {
	if replica.CurrentIncarnation == "" {
		return false
	}

	info := replica.Incarnations[replica.CurrentIncarnation]
	return info.IsRunning()
}

// If a replica does not have a currentIncarnation, it is not running neither pending
func (replica *Replica) IsPending() bool {
	if replica.CurrentIncarnation == "" {
		return false
	}
	info := replica.Incarnations[replica.CurrentIncarnation]
	return info.IsPending()
}

func (replica *Replica) IsBeingAllocated() bool {
	if replica.CurrentIncarnation == "" {
		return false
	}
	info := replica.Incarnations[replica.CurrentIncarnation]
	return info.IsBeingAllocated()
}

type ControllerMetricInfo struct {
	replicas               []*Replica
	SafetyMargin           time.Duration
	MinimumRunningTime     time.Duration
	QoSMeasuringApproach   string
	NumberOfReplicas       int32
	SucceededPods          int32
	ExpectedPodCompletions int32
	// Overwritten by QosDrivenScheduler.GetControllerMetricInfo
	ReferencePod *core.Pod
}

func (cMetricInfo *ControllerMetricInfo) NumberOfRunningPods() int32 {
	total := int32(0)
	for _, replica := range cMetricInfo.replicas {
		if replica.IsRunning() {
			total++
		}
	}
	return total
}

func (cMetricInfo *ControllerMetricInfo) NumberOfPendingPods() int32 {
	total := int32(0)
	for _, replica := range cMetricInfo.replicas {
		if replica.IsPending() {
			total++
		}
	}
	return total
}

func (cMetricInfo *ControllerMetricInfo) NumberOfPodsBeingAllocated() int32 {
	total := int32(0)
	for _, replica := range cMetricInfo.replicas {
		if replica.IsBeingAllocated() {
			total++
		}
	}
	return total
}

// Returns a existing PodMetricInfo or a new one if no known pod matches it's name
// Returns also a boolean that is true if a new PodMetricInfo was returned
func (cMetricInfo *ControllerMetricInfo) GetPodMetricInfo(p *core.Pod) (PodMetricInfo, bool) {
	replicaId, err := cMetricInfo.GetPodReplicaId(p)
	if err != nil {
		return PodMetricInfo{}, true
	}
	return cMetricInfo.replicas[replicaId].Incarnations[PodName(p)], false
}

func (cMetricInfo *ControllerMetricInfo) GetReplica(pod *core.Pod) *Replica {
	replicaId, err := cMetricInfo.GetPodReplicaId(pod)
	if err != nil {
		panic(fmt.Errorf("cannot find incarnations of unalocated pod %s", PodName(pod)))
	}
	return cMetricInfo.replicas[replicaId]
}

// TODO deixar forma de agregar métricas genérica
func (cMetricInfo *ControllerMetricInfo) Metrics(timeRef time.Time, locker sync.Locker) ControllerMetrics {
	qosMeasuringApproach := cMetricInfo.QoSMeasuringApproach
	locker.Lock()
	defer locker.Unlock()

	switch qosMeasuringApproach {
	case AverageQoSMeasuring:
		return CalculateMetricsBasedOnAverageApproach(cMetricInfo, timeRef)
	case IndependentQoSMeasuring:
		return CalculateMetricsBasedOnIndependentApproach(cMetricInfo, timeRef)
	case ConcurrentQosMeasuring:
		return CalculateMetricsBasedOnConcurrentApproach(cMetricInfo, timeRef)
	case AggregateQoSMeasuring:
		return CalculateMetricsBasedOnAggregateApproach(cMetricInfo, timeRef)
	default:
		klog.Errorf("Controller %s associated with %s, which is no valid QoS measure approach.", PodName(cMetricInfo.ReferencePod), qosMeasuringApproach)
		waiting := time.Duration(0)
		binding := time.Duration(0)
		effectiveRunning := time.Duration(0)
		discardedRunning := time.Duration(0)

		//TODO this is the best return in case of invalid QoS measuring approach?
		return ControllerMetrics{
			TimeRef:              timeRef,
			ConMetricInfoRef:     cMetricInfo,
			WaitingTime:          waiting,
			BindingTime:          binding,
			EffectiveRunningTime: effectiveRunning,
			DiscardedRunningTime: discardedRunning,
			EstimatedOverhead:    0,
			SafetyMargin:         cMetricInfo.SafetyMargin,
			QoSMeasuringApproach: qosMeasuringApproach,
			RunningPods:          cMetricInfo.NumberOfRunningPods(),
			PendingPods:          cMetricInfo.NumberOfPendingPods(),
			ParallelPods:         cMetricInfo.NumberOfReplicas,
		}
	}
}

type availabilityEvent struct {
	Time        time.Time
	DeltaOnline int32
}

func CalculateMetricsBasedOnConcurrentApproach(cMetricInfo *ControllerMetricInfo, timeRef time.Time) ControllerMetrics {
	sentinelEvent := availabilityEvent{
		Time:        timeRef,
		DeltaOnline: 0,
	}
	startEvent := func(pMetricInfo *PodMetricInfo) availabilityEvent {
		if !pMetricInfo.StartRunningTime.IsZero() { // Pod ran
			return availabilityEvent{
				Time:        pMetricInfo.StartRunningTime,
				DeltaOnline: 1,
			}
		}
		return sentinelEvent // Pod never ran
	}

	endEvent := func(pMetricInfo *PodMetricInfo) availabilityEvent {
		if !pMetricInfo.EndTime.IsZero() { // Pod ended
			return availabilityEvent{
				Time:        pMetricInfo.EndTime,
				DeltaOnline: -1,
			}
		}
		if !pMetricInfo.StartRunningTime.IsZero() { // Pod running
			return availabilityEvent{
				Time:        timeRef,
				DeltaOnline: -1,
			}
		}
		return sentinelEvent // Pod never ran
	}

	cmpEvent := func(event1, event2 availabilityEvent) bool {
		return event1.Time.Before(event2.Time)
	}

	replicas := cMetricInfo.replicas

	waiting := time.Duration(0)
	binding := time.Duration(0)

	sortedEvents := make([][]availabilityEvent, len(replicas))
	totalPods := 0
	for replicaId, replica := range replicas {
		for _, incarnation := range replica.Incarnations {
			sortedEvents[replicaId] = append(sortedEvents[replicaId], startEvent(&incarnation), endEvent(&incarnation))
			waiting += incarnation.Waiting(timeRef)
			binding += incarnation.Binding(timeRef)
			totalPods++
		}
		sortedEvents[replicaId] = append(sortedEvents[replicaId], sentinelEvent)
		sort.Slice(sortedEvents[replicaId], func(i, j int) bool {
			return cmpEvent(sortedEvents[replicaId][i], sortedEvents[replicaId][j])
		})
	}

	currentIndexes := make([]int, len(sortedEvents))
	totalEvents := 0
	for _, evts := range sortedEvents {
		totalEvents += len(evts)
	}

	effectiveRunning := time.Duration(0)
	discardedRunning := time.Duration(0)
	onlineReplicas := int32(0)
	lastEvent := sentinelEvent // the initial time does not matters, since it will be multiplied for onlineReplicas=0
	for i := 0; i < totalEvents; i++ {
		nextEventReplica := -1
		for replicaId, idx := range currentIndexes {
			if idx >= len(sortedEvents[replicaId]) {
				continue
			}
			if (nextEventReplica < 0) || cmpEvent(sortedEvents[replicaId][idx], sortedEvents[nextEventReplica][currentIndexes[nextEventReplica]]) {
				nextEventReplica = replicaId
			}
		}

		nextEvent := sortedEvents[nextEventReplica][currentIndexes[nextEventReplica]]
		spentTimeWork := nextEvent.Time.Sub(lastEvent.Time) * time.Duration(onlineReplicas)

		if onlineReplicas < cMetricInfo.NumberOfReplicas {
			discardedRunning += spentTimeWork
		} else {
			effectiveRunning += spentTimeWork
		}

		currentIndexes[nextEventReplica]++
		onlineReplicas += nextEvent.DeltaOnline
		lastEvent = nextEvent
	}

	return ControllerMetrics{
		WaitingTime:          waiting,
		BindingTime:          binding,
		EffectiveRunningTime: effectiveRunning,
		DiscardedRunningTime: discardedRunning,
		EstimatedOverhead:    EstimateAllocationOverhead(binding, totalPods),
		SafetyMargin:         cMetricInfo.SafetyMargin,
		QoSMeasuringApproach: ConcurrentQosMeasuring,
		RunningPods:          cMetricInfo.NumberOfRunningPods(),
		ParallelPods:         cMetricInfo.NumberOfReplicas,
	}
}

func CalculateMetricsBasedOnIndependentApproach(cMetricInfo *ControllerMetricInfo, timeRef time.Time) ControllerMetrics {
	waiting := time.Duration(0)
	binding := time.Duration(0)
	effectiveRunning := time.Duration(0)
	discardedRunning := time.Duration(0)
	totalPods := 0

	for _, pMetricInfo := range cMetricInfo.GetReplica(cMetricInfo.ReferencePod).Incarnations {
		waiting += pMetricInfo.Waiting(timeRef)
		binding += pMetricInfo.Binding(timeRef)
		effectiveRunning += pMetricInfo.Running(timeRef)
		totalPods++
	}

	return ControllerMetrics{
		TimeRef:              timeRef,
		ConMetricInfoRef:     cMetricInfo,
		WaitingTime:          waiting,
		BindingTime:          binding,
		EffectiveRunningTime: effectiveRunning,
		DiscardedRunningTime: discardedRunning,
		EstimatedOverhead:    EstimateAllocationOverhead(binding, totalPods),
		SafetyMargin:         cMetricInfo.SafetyMargin,
		MinimumRunningTime:   cMetricInfo.MinimumRunningTime,
		QoSMeasuringApproach: IndependentQoSMeasuring,
		RunningPods:          cMetricInfo.NumberOfRunningPods(),
		PendingPods:          cMetricInfo.NumberOfPendingPods(),
		ParallelPods:         cMetricInfo.NumberOfReplicas,
	}
}

func CalculateMetricsBasedOnAverageApproach(cMetricInfo *ControllerMetricInfo, timeRef time.Time) ControllerMetrics {
	waiting := time.Duration(0)
	binding := time.Duration(0)
	effectiveRunning := time.Duration(0)
	discardedRunning := time.Duration(0)
	totalPods := 0

	for _, replica := range cMetricInfo.replicas {
		for _, pMetricInfo := range replica.Incarnations {
			pMetrics := pMetricInfo.Metrics(timeRef)
			waiting += pMetrics.WaitingTime
			binding += pMetrics.BindingTime
			effectiveRunning += pMetrics.RunningTime
			totalPods++
		}
	}

	return ControllerMetrics{
		TimeRef:              timeRef,
		ConMetricInfoRef:     cMetricInfo,
		WaitingTime:          waiting,
		BindingTime:          binding,
		EffectiveRunningTime: effectiveRunning,
		DiscardedRunningTime: discardedRunning,
		EstimatedOverhead:    EstimateAllocationOverhead(binding, totalPods),
		SafetyMargin:         cMetricInfo.SafetyMargin,
		MinimumRunningTime:   cMetricInfo.MinimumRunningTime,
		QoSMeasuringApproach: AverageQoSMeasuring,
		RunningPods:          cMetricInfo.NumberOfRunningPods(),
		PendingPods:          cMetricInfo.NumberOfPendingPods(),
		ParallelPods:         cMetricInfo.NumberOfReplicas,
	}
}

func CalculateMetricsBasedOnAggregateApproach(cMetricInfo *ControllerMetricInfo, timeRef time.Time) ControllerMetrics {
	waiting := time.Duration(0)
	binding := time.Duration(0)
	effectiveRunning := time.Duration(0)
	discardedRunning := time.Duration(0)
	totalPods := 0

	for _, replica := range cMetricInfo.replicas {
		for _, pMetricInfo := range replica.Incarnations {
			pMetrics := pMetricInfo.Metrics(timeRef)
			waiting += pMetrics.WaitingTime
			binding += pMetrics.BindingTime

			// check if pod is currently effectiveRunning or if it had been succeeded
			if (pMetricInfo.IsRunning()) || (pMetricInfo.IsSucceeded) {
				effectiveRunning += pMetrics.RunningTime
			} else {
				discardedRunning += pMetrics.RunningTime
			}

			totalPods++
		}
	}

	// discarding the running time of reference pod, since it is terminating or being considered for preemption
	referencePod := cMetricInfo.ReferencePod
	if referencePod != nil {
		// if reference pod is currently running, it is being considered for preemption and its running time will be discarded
		if referencePod.Status.Phase == core.PodRunning {
			podInfo, _ := cMetricInfo.GetPodMetricInfo(referencePod)
			referencePodRuntime := podInfo.Running(timeRef)
			effectiveRunning = effectiveRunning - referencePodRuntime
			discardedRunning = discardedRunning + referencePodRuntime
		}
	} else {
		klog.Errorf("[ERROR] There is no reference pod while computing metrics according to aggregate approach.")
	}

	return ControllerMetrics{
		TimeRef:              timeRef,
		ConMetricInfoRef:     cMetricInfo,
		WaitingTime:          waiting,
		BindingTime:          binding,
		EffectiveRunningTime: effectiveRunning,
		DiscardedRunningTime: discardedRunning,
		EstimatedOverhead:    EstimateAllocationOverhead(binding, totalPods),
		SafetyMargin:         cMetricInfo.SafetyMargin,
		MinimumRunningTime:   cMetricInfo.MinimumRunningTime,
		QoSMeasuringApproach: AggregateQoSMeasuring,
		RunningPods:          cMetricInfo.NumberOfRunningPods(),
		PendingPods:          cMetricInfo.NumberOfPendingPods(),
		ParallelPods:         cMetricInfo.NumberOfReplicas,
	}
}

func EstimateAllocationOverhead(bindingTime time.Duration, sampleSize int) time.Duration {
	estimatedAllocationOverhead := time.Millisecond * 2400
	// checking if any pod of this controller has already been allocated
	if bindingTime > time.Duration(0) {
		estimatedAllocationOverhead = time.Duration(bindingTime.Nanoseconds() / int64(sampleSize))
	}
	return estimatedAllocationOverhead
}

type PodMetricInfo struct {
	ControllerName   string
	AllocationStatus string
	LastStatus       *core.Pod
	CreationTime     time.Time
	StartBindingTime time.Time
	StartRunningTime time.Time
	EndTime          time.Time
	IsSucceeded      bool
	// ReplicaId currently is just an informational field
	// and should not be trusted for operational actions
	ReplicaId int
}

func (pMetricInfo *PodMetricInfo) IsRunning() bool {
	return !pMetricInfo.StartRunningTime.IsZero() && pMetricInfo.EndTime.IsZero()
}

func (pMetricInfo *PodMetricInfo) IsPending() bool {
	return !pMetricInfo.CreationTime.IsZero() && pMetricInfo.StartRunningTime.IsZero()
}

func (pMetricInfo *PodMetricInfo) IsBeingAllocated() bool {
	return pMetricInfo.AllocationStatus == AllocatingState
}

func (pMetricInfo *PodMetricInfo) Waiting(now time.Time) time.Duration {
	if pMetricInfo.CreationTime.IsZero() {
		// Pod was not seen added on cache yet
		return 0
	}

	endedWaiting := pMetricInfo.StartBindingTime

	if endedWaiting.IsZero() && !pMetricInfo.StartRunningTime.IsZero() {
		// We've seen the pod running phase from a update,
		// so we might had miss the Binding phase
		endedWaiting = pMetricInfo.StartRunningTime
	}

	if endedWaiting.IsZero() && !pMetricInfo.EndTime.IsZero() {
		// Is possible to listen only the "added" and "deleted" event.
		// In this case we don't know how long the pod waited on queue,
		// so assume it hasn't run.
		endedWaiting = pMetricInfo.EndTime
	}

	if endedWaiting.IsZero() {
		endedWaiting = now
	}

	return endedWaiting.Sub(pMetricInfo.CreationTime)
}

func (pMetricInfo *PodMetricInfo) Binding(now time.Time) time.Duration {
	if pMetricInfo.StartBindingTime.IsZero() {
		// Is assumed pod is in the pending phase, but it can be in the binding phase
		return 0
	}

	endedBinding := pMetricInfo.StartRunningTime

	// Currently PodMetricInfo.StartBindingTime and PodMetricInfo.StartRunningTime
	// are set at the same time, but the code is ready in case this change
	if endedBinding.IsZero() {
		// This would mean the pod is in the binding phase
		endedBinding = now
	}

	return endedBinding.Sub(pMetricInfo.StartBindingTime)
}

func (pMetricInfo *PodMetricInfo) Running(now time.Time) time.Duration {
	if pMetricInfo.StartRunningTime.IsZero() {
		// Pod is in the pending phase yet
		return 0
	}

	endedRunning := pMetricInfo.EndTime

	if endedRunning.IsZero() {
		// Pod is still running
		endedRunning = now
	}

	return endedRunning.Sub(pMetricInfo.StartRunningTime)
}

func (pMetricInfo PodMetricInfo) Metrics(now time.Time) PodMetrics {
	return PodMetrics{
		WaitingTime: pMetricInfo.Waiting(now),
		BindingTime: pMetricInfo.Binding(now),
		RunningTime: pMetricInfo.Running(now),
	}
}

type PodMetrics struct {
	WaitingTime time.Duration
	BindingTime time.Duration
	RunningTime time.Duration
}

type ControllerMetrics struct {
	TimeRef              time.Time
	ConMetricInfoRef     *ControllerMetricInfo
	EffectiveRunningTime time.Duration
	DiscardedRunningTime time.Duration
	WaitingTime          time.Duration
	BindingTime          time.Duration
	EstimatedOverhead    time.Duration
	SafetyMargin         time.Duration
	MinimumRunningTime   time.Duration
	QoSMeasuringApproach string
	RunningPods          int32
	PendingPods          int32
	ParallelPods         int32
}

func (cMetrics *ControllerMetrics) String() string {
	return "{" +
		"RunningTime: " + cMetrics.EffectiveRunningTime.Truncate(time.Millisecond).String() + "," +
		"WaitingTime: " + cMetrics.WaitingTime.Truncate(time.Millisecond).String() + "," +
		"BindingTime: " + cMetrics.BindingTime.Truncate(time.Millisecond).String() + "," +
		"QoS: " + fmt.Sprintf("%.3f", cMetrics.QoS()) +
		"}"
}

func (cMetrics *ControllerMetrics) QoS() float64 {
	effectiveRunning := cMetrics.EffectiveRunningTime.Seconds()
	discardedRunning := cMetrics.DiscardedRunningTime.Seconds()
	waiting := cMetrics.WaitingTime.Seconds()
	binding := cMetrics.BindingTime.Seconds()
	result := effectiveRunning / (effectiveRunning + discardedRunning + waiting + binding)

	if math.IsNaN(result) {
		return 0
	}

	return result
}

func (cMetrics *ControllerMetrics) QoSMetric(pod *core.Pod) float64 {
	qosMeasuringApproach := cMetrics.QoSMeasuringApproach

	switch qosMeasuringApproach {
	case AverageQoSMeasuring:
		return CalculateQoSMetricBasedOnAverageApproach(cMetrics, pod)
	case IndependentQoSMeasuring:
		return CalculateQoSMetricBasedOnIndependentApproach(cMetrics, pod)
	case AggregateQoSMeasuring:
		return CalculateQoSMetricBasedOnAggregateApproach(cMetrics, pod)
	case ConcurrentQosMeasuring:
		return CalculateQosMetricBasedOnConcurrentApproach(cMetrics, pod)

	//case "other":
	//	// TODO add other QoS measuring approaches
	//	fmt.Println("MATCHED CAT")
	default:
		// It is not expected that a controller does not
		// have a valid QoS approach associated here
		klog.Errorf("Pod %s with no valid QoS measure approach associated with.", PodName(pod))
		return math.NaN()
	}
}

func CalculateQoSMetricBasedOnAverageApproach(cMetrics *ControllerMetrics, pod *core.Pod) float64 {

	// check if the pod if of kube-system namespace. These ones are critical pods and have QoS metric -Inf.
	if pod.Namespace == "kube-system" {
		return math.Inf(-1)
	}

	// There is no discarded runtime while using average approach, then all the running time is effectively
	// contributing to increase the availability of the controller
	effectiveRuntime := cMetrics.EffectiveRunningTime.Seconds()
	pendingTime := cMetrics.WaitingTime.Seconds()
	bindingTime := cMetrics.BindingTime.Seconds()
	estimatedOverhead := cMetrics.EstimatedOverhead.Seconds()

	nRunning := cMetrics.RunningPods
	nWaiting := cMetrics.PendingPods
	availability := cMetrics.QoS()

	// the amount of pods that will continue contributing to increase the availability of the controller
	// after this point in time
	residualContribution := nRunning

	// if pod is currently running, it is being considered for preemption
	if pod.Status.Phase == core.PodRunning {
		residualContribution--
	}

	var qosMetric float64
	slo := ControllerSlo(pod)

	if availability == slo {
		qosMetric = 0
	} else if availability < slo {
		qosMetric = effectiveRuntime/slo - (effectiveRuntime + pendingTime + bindingTime) - estimatedOverhead
	} else {
		instantaneousQoS := float64(residualContribution) / float64(nRunning+nWaiting)
		if instantaneousQoS >= slo {
			qosMetric = math.Inf(1)
		} else {
			qosMetric = (effectiveRuntime-slo*(effectiveRuntime+pendingTime+bindingTime))/((float64(nRunning+nWaiting)*slo)-float64(residualContribution)) - estimatedOverhead
		}
	}
	klog.V(1).Infof("[COMPUTE QoS METRIC - AVERAGE] The pod %s: availability %f | runtime %f | pendingTime %f | nRunning: %d | nWaiting: %d | residualContr %d | estimatedOverhead %f | QoSMetric %f",
		pod.Name, availability, effectiveRuntime, pendingTime, nRunning, nWaiting, residualContribution, estimatedOverhead, qosMetric)
	return qosMetric
}

func CalculateQoSMetricBasedOnIndependentApproach(cMetrics *ControllerMetrics, pod *core.Pod) float64 {

	// check if the pod if of kube-system namespace. These ones are critical pods and have QoS metric -Inf.
	if pod.Namespace == "kube-system" {
		return math.Inf(-1)
	}

	// There is no discarded runtime while using independent approach, then all the running time is effectively
	// contributing to increase the availability of the controller
	effectiveRuntime := cMetrics.EffectiveRunningTime.Seconds()
	totalTime := (cMetrics.EffectiveRunningTime + cMetrics.WaitingTime + cMetrics.BindingTime).Seconds()

	return effectiveRuntime/ControllerSlo(pod) - totalTime - cMetrics.EstimatedOverhead.Seconds()
}

func CalculateQosMetricBasedOnConcurrentApproach(cMetrics *ControllerMetrics, pod *core.Pod) float64 {
	// check if the pod if of kube-system namespace. These ones are critical pods and have QoS metric -Inf.
	if pod.Namespace == "kube-system" {
		return math.Inf(-1)
	}

	concurrency := float64(cMetrics.ParallelPods)
	effectiveRuntime := cMetrics.EffectiveRunningTime.Seconds()
	totalTime := (cMetrics.EffectiveRunningTime + cMetrics.DiscardedRunningTime + cMetrics.WaitingTime + cMetrics.BindingTime).Seconds()
	estimatedOverhead := cMetrics.EstimatedOverhead.Seconds()
	slo := ControllerSlo(pod)

	var qosMetric float64
	if availability := effectiveRuntime / totalTime; availability < slo {
		qosMetric = (effectiveRuntime / slo) - totalTime - estimatedOverhead // recoverability
	} else {
		qosMetric = (effectiveRuntime / (concurrency * slo)) - (totalTime / concurrency) // ttv
	}
	return qosMetric
}

func CalculateQoSMetricBasedOnAggregateApproach(cMetrics *ControllerMetrics, pod *core.Pod) float64 {

	// check if the pod if of kube-system namespace. These ones are critical pods and have QoS metric -Inf.
	if pod.Namespace == "kube-system" {
		return math.Inf(-1)
	}

	//cMetrics
	effectiveRuntime := cMetrics.EffectiveRunningTime.Seconds()
	discardedRuntime := cMetrics.DiscardedRunningTime.Seconds()
	pendingTime := cMetrics.WaitingTime.Seconds()
	bindingTime := cMetrics.BindingTime.Seconds()
	estimatedOverhead := cMetrics.EstimatedOverhead.Seconds()

	nRunning := cMetrics.RunningPods
	nWaiting := cMetrics.PendingPods

	// the amount of pods that will continue contributing to increase the availability of the controller
	// after this point in time
	residualContribution := nRunning

	// if pod is currently running, it is being considered for preemption and its running time will be discarded
	if pod.Status.Phase == core.PodRunning {
		residualContribution--

		//podInfo, _ := cMetrics.ConMetricInfoRef.GetPodMetricInfo(pod)
		//currentPodRuntime := podInfo.Running(cMetrics.TimeRef).Seconds()
		//effectiveRuntime = effectiveRuntime - currentPodRuntime
		//discardedRuntime = discardedRuntime + currentPodRuntime
	}

	// computing availability discarding the runtime of the currentPod
	//availability := effectiveRuntime / (effectiveRuntime + discardedRuntime + pendingTime + bindingTime)
	availability := cMetrics.QoS()

	if math.IsNaN(availability) {
		availability = 0
	}

	var qosMetric float64
	slo := ControllerSlo(pod)

	if availability == slo {
		qosMetric = 0
	} else if availability < slo {
		qosMetric = effectiveRuntime/slo - (effectiveRuntime + discardedRuntime + pendingTime + bindingTime) - estimatedOverhead
	} else {
		instantaneousQoS := float64(residualContribution) / float64(nRunning+nWaiting)
		if instantaneousQoS >= slo {
			qosMetric = math.Inf(1)
		} else {
			qosMetric = (effectiveRuntime-slo*(effectiveRuntime+discardedRuntime+pendingTime+bindingTime))/((float64(nRunning+nWaiting)*slo)-float64(residualContribution)) - estimatedOverhead
		}
	}

	klog.V(1).Infof("[COMPUTING QoS METRIC - AGGREGATE] The pod %s (%s): availability %f | effectiveRuntime %f | discardedRuntime %f | pendingTime %f | nRunning: %d | nWaiting: %d | residualContr %d | estimatedOverhead %f | QoSMetric %f",
		pod.Name, pod.Status.Phase, availability, effectiveRuntime, discardedRuntime, pendingTime, nRunning, nWaiting, residualContribution, estimatedOverhead, qosMetric)
	return qosMetric
}

func (cMetrics *ControllerMetrics) PreemptionOverhead() float64 {
	allocation := cMetrics.BindingTime.Seconds()
	effectiveRunning := cMetrics.EffectiveRunningTime.Seconds()
	discardedRunning := cMetrics.DiscardedRunningTime.Seconds()

	// TODO we need to think better about it. Should we consider discardedRunning as wasted resource in
	//  case of concurrent approach as well? Our PoC did not consider, only with aggregate approach.
	if cMetrics.QoSMeasuringApproach == AggregateQoSMeasuring {
		return (allocation + discardedRunning) / (allocation + effectiveRunning + discardedRunning)
	}
	return allocation / (allocation + effectiveRunning + discardedRunning)
}

type CloneableTime struct {
	time.Time
}

func (t CloneableTime) Clone() framework.StateData {
	return t
}

func ControllerSlo(p *core.Pod) float64 {
	slo, err := strconv.ParseFloat(p.Annotations[SloAnnotation], 64)
	if err != nil {
		return DefaultSlo
	}
	return slo
}

func ControllerImportance(p *core.Pod) float64 {
	return ControllerSlo(p)
}

func ControllerName(p *core.Pod) string {
	return p.Annotations[ControllerAnnotation]
}

func ControllerQoSMeasuring(pod *core.Pod) string {
	// The QoS measuring approach was set in pod template
	configuredQoSMeasuring := pod.Annotations[QoSMeasuringAnnotation]
	if configuredQoSMeasuring != "" {
		return configuredQoSMeasuring
	}

	// If QoS measuring approach was not set in pod template, use the default one according to
	// class of controller
	if controllerRef := metav1.GetControllerOf(pod); controllerRef == nil {
		klog.V(1).Infof("The is no controller associated with pod %s", pod.Name)
		return IndependentQoSMeasuring
	} else {
		if controllerRef.Kind == "Job" {
			klog.V(1).Infof("The aggregate is default QoS measuring approach for job %s", controllerRef.Name)
			return AggregateQoSMeasuring
		} else {
			klog.V(1).Infof("The average is default QoS measuring approach for replication controller %s", controllerRef.Name)
			return AverageQoSMeasuring
		}
	}
}

// TODO: should this be a method of ControllerMetrics?
func (scheduler *QosDrivenScheduler) AcceptablePreemptionOverhead(p *core.Pod) float64 {
	acceptableOverhead, err := strconv.ParseFloat(p.Annotations[AcceptablePreemptionOverheadAnnotation], 64)
	if err != nil {
		acceptableOverhead = scheduler.Args.AcceptablePreemptionOverhead
	}
	return math.Min(1-ControllerSlo(p), acceptableOverhead)
}

func PodName(p *core.Pod) string {
	return util.GetPodFullName(p)
}

// TODO an optimization would be look in the most recent pods first
func (cMetricInfo ControllerMetricInfo) GetPodReplicaId(p *core.Pod) (int, error) {
	podName := PodName(p)

	for replicaId, replica := range cMetricInfo.replicas {
		if replica.CurrentIncarnation == podName {
			return replicaId, nil
		}
	}

	for replicaId, replica := range cMetricInfo.replicas {
		if _, found := replica.Incarnations[podName]; found {
			return replicaId, nil
		}
	}

	return -1, fmt.Errorf("pod %s wasn't found in any replicaId", podName)
}

func (cMetricInfo *ControllerMetricInfo) AllocateReplicaId(pod *core.Pod) int {
	for replicaId, replica := range cMetricInfo.replicas {
		if replica.CurrentIncarnation == "" {
			replica.CurrentIncarnation = PodName(pod)
			return replicaId
		}
	}
	panic(fmt.Errorf("no free replicaId was found to pod %s", PodName(pod)))
}

func (cMetricInfo *ControllerMetricInfo) DeallocateReplicaId(pod *core.Pod) {
	replicaId, err := cMetricInfo.GetPodReplicaId(pod)
	if err != nil {
		klog.Errorf("Pod %s wasn't found in any replicaId while it was trying to deallocate its "+
			"replicaId.", PodName(pod))
	}
	cMetricInfo.replicas[replicaId].CurrentIncarnation = ""
}

func (cMetricInfo *ControllerMetricInfo) Clone() ControllerMetricInfo {
	return ControllerMetricInfo{
		replicas:               cMetricInfo.replicas,
		SafetyMargin:           cMetricInfo.SafetyMargin,
		MinimumRunningTime:     cMetricInfo.MinimumRunningTime,
		QoSMeasuringApproach:   cMetricInfo.QoSMeasuringApproach,
		NumberOfReplicas:       cMetricInfo.NumberOfReplicas,
		SucceededPods:          cMetricInfo.SucceededPods,
		ExpectedPodCompletions: cMetricInfo.ExpectedPodCompletions,
		ReferencePod:           cMetricInfo.ReferencePod,
	}
}

const (
	BindingStart                           = "BindingStart"
	ControllerAnnotation                   = "controller"
	SloAnnotation                          = "slo"
	AcceptablePreemptionOverheadAnnotation = "acceptablePreemptionOverhead"
	QoSMeasuringAnnotation                 = "qosMeasuring"

	//Allocation states
	AllocatingState = "allocating"
	AllocatedState  = "allocated"

	//QoS measuring approach
	AverageQoSMeasuring     = "average"
	IndependentQoSMeasuring = "independent"
	AggregateQoSMeasuring   = "aggregate"
	ConcurrentQosMeasuring  = "concurrent"
)
