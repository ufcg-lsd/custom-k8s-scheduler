package qos_driven_scheduler

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"

	"math"
	"sort"
	"sync"
	"time"
)

var _ framework.PostFilterPlugin = &QosDrivenScheduler{}

// This is the method required to the PostFilterPlugin interface.
// It is called to a pod after it failed at filtering phase.
// It will try to make room for the pod by preempting lower precedence pods.
func (scheduler *QosDrivenScheduler) PostFilter(ctx context.Context, state *framework.CycleState, pod *core.Pod, m framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	//	preemptionStartTime := time.Now()
	defer func() {
		metrics.PreemptionAttempts.Inc()
		//		metrics.SchedulingAlgorithmPreemptionEvaluationDuration.Observe(metrics.SinceInSeconds(preemptionStartTime))
		//		metrics.DeprecatedSchedulingDuration.WithLabelValues(metrics.PreemptionEvaluation).Observe(metrics.SinceInSeconds(preemptionStartTime))
	}()

	nominatedNode, err := scheduler.preempt(ctx, state, pod, m)
	if err != nil {
		return nil, framework.NewStatus(framework.Error, err.Error())
	}
	if nominatedNode == "" {
		return nil, framework.NewStatus(framework.Unschedulable)
	}
	return framework.NewPostFilterResultWithNominatedNode(nominatedNode), framework.NewStatus(framework.Success)
}

// preempt finds nodes with pods that can be preempted to make room for "pod" to
// schedule. It chooses one of the nodes and preempts the pods on the node and
// returns 1) the node name which is picked up for preemption, 2) any possible error.
// preempt does not update its snapshot. It uses the same snapshot used in the
// scheduling cycle. This is to avoid a scenario where preempt finds feasible
// nodes without preempting any pod. When there are many pending pods in the
// scheduling queue a nominated pod will go back to the queue and behind
// other pods with higher precedence. The nominated pod prevents other pods from
// using the nominated resources and the nominated pod could take a long time
// before it is retried after many other pending pods.
func (scheduler *QosDrivenScheduler) preempt(ctx context.Context, state *framework.CycleState, pod *core.Pod, m framework.NodeToStatusMap) (string, error) {
	fh := scheduler.fh
	cs := fh.ClientSet()

	var err error

	// Tenta obter a versão atualizada do pod diretamente do API Server
	updatedPod, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get updated pod %s/%s: %v", pod.Namespace, pod.Name, err)
		return "", fmt.Errorf("failed to get updated pod: %w", err)
	}
	klog.V(4).Infof("Successfully retrieved updated pod %s/%s", updatedPod.Namespace, updatedPod.Name)
	pod = updatedPod

	if !scheduler.podEligibleToPreemptOthers(pod, fh.SnapshotSharedLister().NodeInfos(), m[pod.Status.NominatedNodeName]) {
		klog.V(5).Infof("Pod %v/%v is not eligible for more preemption.", pod.Namespace, pod.Name)
		return "", nil
	}
	allNodes, err := fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return "", err
	}
	if len(allNodes) == 0 {
		return "", fmt.Errorf("no nodes available for scheduling")

	}
	potentialNodes := nodesWherePreemptionMightHelp(allNodes, m)
	if len(potentialNodes) == 0 {
		klog.V(3).Infof("Preemption will not help schedule pod %v/%v on any node.", pod.Namespace, pod.Name)
		// In this case, we should clean-up any existing nominated node name of the pod.
		if err := util.ClearNominatedNodeName(ctx, cs, pod); err != nil {
			klog.Errorf("Cannot clear 'NominatedNodeName' field of pod %v/%v: %v", pod.Namespace, pod.Name, err)
			// We do not return as this error is not critical.
		}
		return "", nil
	}
	if klog.V(5).Enabled() {
		var sample []string
		for i := 0; i < 10 && i < len(potentialNodes); i++ {
			sample = append(sample, potentialNodes[i].Node().Name)
		}
		klog.Infof("%v potential nodes for preemption, first %v are: %v", len(potentialNodes), len(sample), sample)
	}

	//TODO Will we continue checking pdbs? (we kept this verification on old PoC code)
	pdbs, err := scheduler.getPodDisruptionBudgets()
	if err != nil {
		return "", err
	}
	nodeNameToVictims, err := scheduler.selectNodesForPreemption(ctx, fh, state, pod, potentialNodes, pdbs)
	if err != nil {
		return "", err
	}

	// We will only check nodeNameToVictims with extenders that support preemption.
	// Extenders which do not support preemption may later prevent preemptor from being scheduled on the nominated
	// node. In that case, scheduler will find a different host for the preemptor in subsequent scheduling cycles.
	nodeNameToVictims, err = processPreemptionWithExtenders(fh, pod, nodeNameToVictims)
	if err != nil {
		return "", err
	}

	// TODO It is possible to configure the preemption cost function by parametrizing this function.
	//  Currently, we support only one preemption cost, but we could support several ones by receiving that one to be used through
	//  scheduler arguments. At this point, we could select one of the candidate nodes based on the configured preemption cost function.
	candidateNode := scheduler.pickOneNodeForPreemption(nodeNameToVictims)
	if len(candidateNode) == 0 {
		klog.Infof("[PREEMPT CALL] No node was chosen to allocate pod %s", pod.Name)
		return "", nil
	}
	klog.Infof("[PREEMPT CALL] The node %v  was chosen to allocate pod %s", candidateNode, pod.Name)

	victims := nodeNameToVictims[candidateNode].Pods
	for _, victim := range victims {
		if err := util.DeletePod(ctx, cs, victim); err != nil {
			klog.Errorf("Error preempting pod %v/%v: %v", victim.Namespace, victim.Name, err)
			return "", err
		}
		// If the victim is a WaitingPod, send a reject message to the PermitPlugin
		if waitingPod := fh.GetWaitingPod(victim.UID); waitingPod != nil {
			waitingPod.Reject("preempted", "Preemption: Pod foi pré-empregado para liberar recursos.")
		}
		klog.V(5).Infof("%s preempted by %s on node %v", PodName(victim), PodName(pod), candidateNode)
		fh.EventRecorder().Eventf(victim, pod, core.EventTypeNormal, "Preempted", "Preempting", "Preempted by %v/%v on node %v", pod.Namespace, pod.Name, candidateNode)
	}
	metrics.PreemptionVictims.Observe(float64(len(victims)))

	// Lower precedence pods nominated to run on this node, may no longer fit on
	// this node. So, we should remove their nomination. Removing their
	// nomination updates these pods and moves them to the active queue. It
	// lets scheduler find another place for them.
	nominatedPods := scheduler.getLowerPrecedenceNominatedPods(fh, pod, candidateNode)
	if err := util.ClearNominatedNodeName(ctx, cs, nominatedPods...); err != nil {
		klog.Errorf("Cannot clear 'NominatedNodeName' field: %v", err)
		// We do not return as this error is not critical.
	}

	return candidateNode, nil
}

// podEligibleToPreemptOthers determines whether this pod should be considered
// for preempting other pods or not. If this pod has already preempted other
// pods and those are in their graceful termination period, it shouldn't be
// considered for preemption.
// We look at the node that is nominated for this pod and as long as there are
// terminating pods on the node, we don't consider this for preempting more pods.
func (scheduler *QosDrivenScheduler) podEligibleToPreemptOthers(pod *core.Pod, nodeInfos framework.NodeInfoLister, nominatedNodeStatus *framework.Status) bool {
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == core.PreemptNever {
		klog.V(5).Infof("Pod %v/%v is not eligible for preemption because it has a preemptionPolicy of %v", pod.Namespace, pod.Name, core.PreemptNever)
		return false
	}
	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		// If the pod's nominated node is considered as UnschedulableAndUnresolvable by the filters,
		// then the pod should be considered for preempting again.
		if nominatedNodeStatus.Code() == framework.UnschedulableAndUnresolvable {
			return true
		}

		if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
			for _, p := range nodeInfo.Pods {
				if p.Pod.DeletionTimestamp != nil && scheduler.HigherPrecedence(pod, p.Pod) {
					// There is a terminating pod on the nominated node.
					klog.V(1).Infof("Victim candidate %s is already terminating, skipping preemption.", PodName(p.Pod))
					return false
				}
			}
		}
	}
	return true
}

// nodesWherePreemptionMightHelp returns a list of nodes with failed predicates
// that may be satisfied by removing pods from the node.
func nodesWherePreemptionMightHelp(nodes []*framework.NodeInfo, m framework.NodeToStatusMap) []*framework.NodeInfo {
	var potentialNodes []*framework.NodeInfo
	for _, node := range nodes {
		name := node.Node().Name
		// We reply on the status by each plugin - 'Unschedulable' or 'UnschedulableAndUnresolvable'
		// to determine whether preemption may help or not on the node.
		if m[name].Code() == framework.UnschedulableAndUnresolvable {
			continue
		}
		potentialNodes = append(potentialNodes, node)
	}
	return potentialNodes
}

func cloneNodeInfo(original *framework.NodeInfo) *framework.NodeInfo {
	// Cria uma nova instância de NodeInfo
	clonedNodeInfo := framework.NewNodeInfo()

	// Copia o Node associado
	clonedNodeInfo.SetNode(original.Node())

	// Copia os pods associados
	for _, podInfo := range original.Pods {
		clonedNodeInfo.AddPod(podInfo.Pod)
	}

	// Retorna a cópia
	return clonedNodeInfo
}

// selectNodesForPreemption finds all the nodes with possible victims for
// preemption in parallel.
func (scheduler *QosDrivenScheduler) selectNodesForPreemption(
	ctx context.Context,
	fh framework.Handle,
	state *framework.CycleState,
	pod *core.Pod,
	potentialNodes []*framework.NodeInfo,
	pdbs []*policy.PodDisruptionBudget,
) (map[string]*extenderv1.Victims, error) {
	nodeNameToVictims := map[string]*extenderv1.Victims{}
	var resultLock sync.Mutex

	checkNode := func(i int) {
		nodeInfoCopy := cloneNodeInfo(potentialNodes[i])

		stateCopy := state.Clone()
		pods, numPDBViolations, fits := scheduler.selectVictimsOnNode(ctx, fh, stateCopy, pod, nodeInfoCopy, pdbs)

		//TODO Remove this code! (only for debugging)
		klog.V(1).Infof("[SELECTING NODE FOR PREEMPTION] Pod %s fits on node %s? %t", pod.Name, potentialNodes[i].Node().Name, fits)

		if fits {
			resultLock.Lock()
			victims := extenderv1.Victims{
				Pods:             pods,
				NumPDBViolations: int64(numPDBViolations),
			}
			nodeNameToVictims[potentialNodes[i].Node().Name] = &victims
			resultLock.Unlock()
		}
	}
	ParallelizeUntil(ctx, len(potentialNodes), checkNode)
	return nodeNameToVictims, nil
}

// processPreemptionWithExtenders processes preemption with extenders
func processPreemptionWithExtenders(fh framework.Handle, pod *core.Pod, nodeNameToVictims map[string]*extenderv1.Victims) (map[string]*extenderv1.Victims, error) {
	if len(nodeNameToVictims) > 0 {
		for _, extender := range fh.Extenders() {
			if extender.SupportsPreemption() && extender.IsInterested(pod) {
				newNodeNameToVictims, err := extender.ProcessPreemption(
					pod,
					nodeNameToVictims,
					fh.SnapshotSharedLister().NodeInfos(),
				)
				if err != nil {
					if extender.IsIgnorable() {
						klog.Warningf("Skipping extender %v as it returned error %v and has ignorable flag set",
							extender, err)
						continue
					}
					return nil, err
				}

				// Replace nodeNameToVictims with new result after preemption. So the
				// rest of extenders can continue use it as parameter.
				nodeNameToVictims = newNodeNameToVictims

				// If node list becomes empty, no preemption can happen regardless of other extenders.
				if len(nodeNameToVictims) == 0 {
					break
				}
			}
		}
	}

	return nodeNameToVictims, nil
}

// pickOneNodeForPreemption chooses one node among the given nodes.
// This code picks the code with the highest preemption score based on the QoS metric of the victims.
// It favors the preemption of pods associated with higher QoS metric (TTV). In case of preemption of pods closer
// to violate their SLOs or pods that are already violating their SLOs (QoS metric < safetyMargin), it favors to preempt pods of
// lower important classes (based on the importance of their controllers).
func (scheduler *QosDrivenScheduler) pickOneNodeForPreemption(nodesToVictims map[string]*extenderv1.Victims) string {
	if len(nodesToVictims) == 0 {
		return ""
	}

	// This constant is used to represent pods of any class that are not so close to violate their SLOs (QoS metric > safetyMargin)
	const FAR_FROM_VIOLATING_CLASS = -1

	nodeToPreemptionScore := make(map[string]map[float64]float64)
	victimClasses := make(map[float64]bool)

	// preemption score of general pods
	victimClasses[FAR_FROM_VIOLATING_CLASS] = true

	timeRef := time.Now()

	for node, victims := range nodesToVictims {
		if len(victims.Pods) == 0 {
			return node
		}

		// This data structure is used to select the most appropriate host based on multicriteria. Each class of potential pod
		// victim has an entry with negative scores (if there is one or more victim closer of violating its SLO or it is already
		// violating its SLO -- QoS metric < safetyMargin). The class of a pod is given by its importance.
		// There is also an entry for general score, which is related to potential victims that are not closer
		// of violating their SLOs (QoS metric >= safetyMargin).
		victimClassToScore := make(map[float64]float64)
		var generalScore float64

		for _, p := range victims.Pods {
			safetyMargin := scheduler.Args.SafetyMargin.Duration.Seconds()
			controllerMetricInfo := scheduler.GetControllerMetricInfo(p)
			controllerMetric := controllerMetricInfo.Metrics(timeRef, scheduler.lock.RLocker())

			podQoSMetric := controllerMetric.QoSMetric(p)
			podImportance := ControllerImportance(p)

			var preemptionScore float64

			// If the allocated pod is not contributing to increase the QoS of its controller,
			// there is no preemption cost associated with it
			if scheduler.isPodContributingToImproveControllerQoS(p) {
				preemptionScore = podQoSMetric - safetyMargin
			} else {
				preemptionScore = 0
			}

			// the preemption score is negative
			if podQoSMetric < safetyMargin {
				victimClasses[podImportance] = true
				victimClassToScore[podImportance] += preemptionScore
			}
			generalScore += preemptionScore
		}

		victimClassToScore[FAR_FROM_VIOLATING_CLASS] = generalScore
		nodeToPreemptionScore[node] = victimClassToScore

		// TODO remove in the futute (only for debugging)
		klog.V(1).Infof("[PICKING ONE NODE FOR PREEMPTION] Node %s -> preemption score: [%v]", node, victimClassToScore)
	}

	// It is possible that some victim classes are present only on some nodes. In order to compute the preemption score adequately,
	// we need to normalize the preemption score of all nodes. It means that if there are not victims of a specific class in a node,
	// the preemption score of this class must be zero.
	var sortedVictimClasses []float64

	for slo := range victimClasses {
		for _, preemptionScore := range nodeToPreemptionScore {
			_, exists := preemptionScore[slo]
			if !exists {
				preemptionScore[slo] = 0
			}
		}

		sortedVictimClasses = append(sortedVictimClasses, slo)
	}

	// Sorting the victims classes from the most important to the less important (pods that are not closer to violate their
	// SLO are the less important ones)
	sort.Slice(sortedVictimClasses, func(i, j int) bool { return sortedVictimClasses[i] > sortedVictimClasses[j] })

	klog.V(1).Infof("[PICKING ONE NODE FOR PREEMPTION] After preemption score normalization: [%v]", nodeToPreemptionScore)
	klog.V(1).Infof("[PICKING ONE NODE FOR PREEMPTION] All preemptable SLOs: [%v]", sortedVictimClasses)

	// Initially, all nodes are candidates to be selected
	var candidateNodes []string
	for node := range nodeToPreemptionScore {
		candidateNodes = append(candidateNodes, node)
	}

	// If there is only one candidate node, it should be choosen
	if len(candidateNodes) == 1 {
		return candidateNodes[0]
	}

	// Filter nodes by highest preemption score
	for _, class := range sortedVictimClasses {
		candidateNodes = scheduler.filterNodesByHighestPreemptionScore(candidateNodes, class, nodeToPreemptionScore)

		if len(candidateNodes) == 1 {
			return candidateNodes[0]
		}
	}

	// There are a few nodes with maximum preemption score.
	// Find one with the minimum number of pods.
	minPreemptedPods := math.MaxInt32
	var minPreemptedPodNodes []string
	for _, node := range candidateNodes {

		numPods := len(nodesToVictims[node].Pods)
		if numPods < minPreemptedPods {
			minPreemptedPods = numPods
			minPreemptedPodNodes = nil
		}
		if numPods == minPreemptedPods {
			minPreemptedPodNodes = append(minPreemptedPodNodes, node)
		}
	}

	// At this point, even if there are more than one node with the same score,
	// return the first one.
	if len(minPreemptedPodNodes) > 0 {
		return minPreemptedPodNodes[0]
	}

	klog.Errorf("Error in logic of node scoring for preemption. We should never reach here!")
	return ""
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

// selectVictimsOnNode finds minimum set of pods on the given node that should
// be preempted in order to make enough room for "pod" to be scheduled.
// The algorithm first checks if the pod can be scheduled on the node when all the
// lower precedence pods are gone. If so, it sorts all the lower precedence pods by
// their precedence and then puts them into two groups of those whose PodDisruptionBudget
// will be violated if preempted and other non-violating pods. Both groups are
// sorted by precedence. It first tries to reprieve as many PDB violating pods as
// possible and then does them same for non-PDB-violating pods while checking
// that the "pod" can still fit on the node.
// NOTE: This function assumes that it is never called if "pod" cannot be scheduled
// due to pod affinity, node affinity, or node anti-affinity reasons. None of
// these predicates can be satisfied by removing more pods from the node.
func (scheduler *QosDrivenScheduler) selectVictimsOnNode(
	ctx context.Context,
	ph framework.Handle,
	state *framework.CycleState,
	pod *core.Pod,
	nodeInfo *framework.NodeInfo,
	pdbs []*policy.PodDisruptionBudget,
) ([]*core.Pod, int, bool) {
	var potentialVictims []*core.Pod

	removePod := func(rp *core.Pod) error {
		if err := nodeInfo.RemovePod(klog.Background(), rp); err != nil {
			return err
		}
		podInfo, err := framework.NewPodInfo(rp)
		if err != nil {
			return err
		}
		status := ph.RunPreFilterExtensionRemovePod(ctx, state, pod, podInfo, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}

	addPod := func(ap *core.Pod) error {
		podInfo, err := framework.NewPodInfo(ap)
		if err != nil {
			return err
		}
		nodeInfo.AddPod(ap)
		status := ph.RunPreFilterExtensionAddPod(ctx, state, pod, podInfo, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}
	// As the first step, remove all the lower precedence pods from the node and
	// check if the given pod can be scheduled.
	for _, p := range nodeInfo.Pods {
		//  It is not possible to preempt a pod of kube-system namespace.
		if p.Pod.Namespace != "kube-system" &&
			(!scheduler.isPodContributingToImproveControllerQoS(p.Pod) || scheduler.HigherPrecedence(pod, p.Pod)) &&
			scheduler.CanPreempt(pod, p.Pod) {

			potentialVictims = append(potentialVictims, p.Pod)
			if err := removePod(p.Pod); err != nil {
				return nil, 0, false
			}
		}
	}
	// If the new pod does not fit after removing all the lower precedence pods,
	// we are almost done and this node is not suitable for preemption. The only
	// condition that we could check is if the "pod" is failing to schedule due to
	// inter-pod affinity to one or more victims, but we have decided not to
	// support this case for performance reasons. Having affinity to lower
	// precedence pods is not a recommended configuration anyway.
	fits := true
	status := ph.RunFilterPlugins(ctx, state, pod, nodeInfo)
	if !status.IsSuccess() {
		fits = false
		if status.Code() == framework.Error {
			klog.Warningf("Encountered error while selecting victims on node %v: %v", nodeInfo.Node().Name, status.Message())
		}
	}
	if !fits {
		return nil, 0, false
	}

	var victims []*core.Pod
	numViolatingVictim := 0
	sort.Slice(potentialVictims, func(i, j int) bool {
		pod1 := potentialVictims[i]
		pod2 := potentialVictims[j]

		// pods that are not contributing to improve the QoS of their controllers are less important than other ones
		pod1IsContributingToQoS := scheduler.isPodContributingToImproveControllerQoS(pod1)
		pod2IsContributingToQoS := scheduler.isPodContributingToImproveControllerQoS(pod2)

		if pod1IsContributingToQoS && !pod2IsContributingToQoS {
			return true
		}

		if !pod1IsContributingToQoS && pod2IsContributingToQoS {
			return false
		}

		return scheduler.HigherPrecedence(pod1, pod2)
	})

	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest precedence victims.
	violatingVictims, nonViolatingVictims := filterPodsWithPDBViolation(potentialVictims, pdbs)
	reprievePod := func(p *core.Pod) (bool, error) {
		if err := addPod(p); err != nil {
			return false, err
		}
		status := ph.RunFilterPlugins(ctx, state, pod, nodeInfo)
		if !status.IsSuccess() {
			if err := removePod(p); err != nil {
				return false, err
			}
			victims = append(victims, p)
			klog.V(5).Infof("Pod %v/%v is a potential preemption victim on node %v.", p.Namespace, p.Name, nodeInfo.Node().Name)
			return false, nil
		}
		return true, nil
	}

	// TODO We need to check better the following two for structures
	for _, p := range violatingVictims {
		if fits, err := reprievePod(p); err != nil {
			klog.Warningf("Failed to reprieve pod %q: %v", p.Name, err)
			return nil, 0, false
		} else if !fits {
			numViolatingVictim++
		}
	}
	// Now we try to reprieve non-violating victims.
	for _, p := range nonViolatingVictims {
		if _, err := reprievePod(p); err != nil {
			klog.Warningf("Failed to reprieve pod %q: %v", p.Name, err)
			return nil, 0, false
		}
	}
	return victims, numViolatingVictim, true
}

// isPodContributingToImproveControllerQoS receives a pod and return false if it doesn't contribute to increase
// the QoS of its controller, this happen only when the pod is associated with a controller that use
// the concurrent qos measuring approach and at least one pod from the same controller is not running
//
// this function assumes that the received pod is running.
func (scheduler *QosDrivenScheduler) isPodContributingToImproveControllerQoS(pod *core.Pod) bool {
	// These ones are critical pods and they always be considered as very important.
	if pod.Namespace == "kube-system" {
		return true
	}

	cMetricInfo := scheduler.GetControllerMetricInfo(pod)

	if cMetricInfo.QoSMeasuringApproach == ConcurrentQosMeasuring {
		scheduler.lock.RLock()
		defer scheduler.lock.RUnlock()
		klog.V(1).Infof("[CHECKING POD CONTRIBUTES TO QOS]: concurrent controller %s | replication %d | running_pods %d | allocating_pods %d", ControllerName(pod),
			cMetricInfo.NumberOfReplicas, cMetricInfo.NumberOfRunningPods(), cMetricInfo.NumberOfPodsBeingAllocated())
		return cMetricInfo.NumberOfRunningPods()+cMetricInfo.NumberOfPodsBeingAllocated() == cMetricInfo.NumberOfReplicas
	}
	return true
}

// getLowerPrecedenceNominatedPods returns pods whose precedence is smaller than the
// precedence of the given "pod" and are nominated to run on the given node.
// Note: We could possibly check if the nominated lower precedence pods still fit
// and return those that no longer fit, but that would require lots of
// manipulation of NodeInfo and PreFilter state per nominated pod. It may not be
// worth the complexity, especially because we generally expect to have a very
// small number of nominated pods per node.
func (scheduler *QosDrivenScheduler) getLowerPrecedenceNominatedPods(pn framework.PodNominator, pod *core.Pod, nodeName string) []*core.Pod {
	pods := pn.NominatedPodsForNode(nodeName)

	if len(pods) == 0 {
		return nil
	}

	var lowerPrecedencePods []*core.Pod
	for _, p := range pods {
		// TODO Do we need to check the mechanisms for limiting preemption here too? Probably not
		if scheduler.HigherPrecedence(pod, p.Pod) {
			lowerPrecedencePods = append(lowerPrecedencePods, p.Pod)
		}
	}
	return lowerPrecedencePods
}

// filterPodsWithPDBViolation groups the given "pods" into two groups of "violatingPods"
// and "nonViolatingPods" based on whether their PDBs will be violated if they are
// preempted.
// This function is stable and does not change the order of received pods. So, if it
// receives a sorted list, grouping will preserve the order of the input list.
func filterPodsWithPDBViolation(pods []*core.Pod, pdbs []*policy.PodDisruptionBudget) (violatingPods, nonViolatingPods []*core.Pod) {
	pdbsAllowed := make([]int32, len(pdbs))
	for i, pdb := range pdbs {
		pdbsAllowed[i] = pdb.Status.DisruptionsAllowed
	}

	for _, obj := range pods {
		pod := obj
		pdbForPodIsViolated := false
		// A pod with no labels will not match any PDB. So, no need to check.
		if len(pod.Labels) != 0 {
			for i, pdb := range pdbs {
				if pdb.Namespace != pod.Namespace {
					continue
				}
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					continue
				}
				// A PDB with a nil or empty selector matches nothing.
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}

				// Existing in DisruptedPods means it has been processed in API server,
				// we don't treat it as a violating case.
				if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
					continue
				}
				// Only decrement the matched pdb when it's not in its <DisruptedPods>;
				// otherwise we may over-decrement the budget number.
				pdbsAllowed[i]--
				// We have found a matching PDB.
				if pdbsAllowed[i] < 0 {
					pdbForPodIsViolated = true
				}
			}
		}
		if pdbForPodIsViolated {
			violatingPods = append(violatingPods, pod)
		} else {
			nonViolatingPods = append(nonViolatingPods, pod)
		}
	}
	return violatingPods, nonViolatingPods
}

func (scheduler *QosDrivenScheduler) getPodDisruptionBudgets() ([]*policy.PodDisruptionBudget, error) {
	pdbLister := scheduler.fh.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister()
	pdbs, err := pdbLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Error listing PodDisruptionBudgets: %v", err)
		return nil, err
	}
	return pdbs, nil
}

// Checks if pendingPod can preempt allocatedPod considering their importance and preemption overhead
// It's assumed that schedule.HigherPrecedence(pendingPod, allocatedPod) == true
func (scheduler *QosDrivenScheduler) CanPreempt(pendingPod, allocatedPod *core.Pod) bool {
	now := time.Now()

	// check if both pods are associated with the same controller and QoS measuring approach is not independent
	if (ControllerName(pendingPod) == ControllerName(allocatedPod)) &&
		(ControllerQoSMeasuring(pendingPod) != IndependentQoSMeasuring) {
		klog.V(1).Infof("Pods %s and %s are associated with the same controller and QoS measuring is %s --> %s can not be preempted",
			allocatedPod.Name, pendingPod.Name, ControllerQoSMeasuring(pendingPod), allocatedPod.Name)
		return false
	}

	// if pod is not contributing to improve the controller QoS, it is preemptable
	if !scheduler.isPodContributingToImproveControllerQoS(allocatedPod) {
		return true
	}

	cMetricInfo := scheduler.GetControllerMetricInfo(allocatedPod)
	cMetrics := cMetricInfo.Metrics(now, scheduler.lock.RLocker())
	pMetrics := scheduler.getUpdatedVersion(allocatedPod)

	// In this way, the pod that will be preempted could stay pending at least for minRunningTime before be able to preempt less important pods again
	//minMarginToBePreempted := cMetrics.SafetyMargin + cMetrics.MinimumRunningTime

	// If it's not a resource contention scenario and the pod already run the minimum time
	isMinimalRunningTimeAchieved := pMetrics.Running(now) >= cMetrics.MinimumRunningTime
	//if isMinimalRunningTimeAchieved && (cMetrics.QoSMetric(allocatedPod) >= minMarginToBePreempted.Seconds()) {
	if isMinimalRunningTimeAchieved && (cMetrics.QoSMetric(allocatedPod) >= cMetrics.SafetyMargin.Seconds()) {
		return true
	}

	// If the pending pod is more important
	if ControllerImportance(pendingPod) > ControllerImportance(allocatedPod) {
		return true
	}

	// A pod should not preempt other of same importance
	// that is violating it's allowed preemption overhead
	// or that hasn't run its minimum running time
	podHasAnAcceptableOverhead := cMetrics.PreemptionOverhead() <= scheduler.AcceptablePreemptionOverhead(allocatedPod)
	if isMinimalRunningTimeAchieved && podHasAnAcceptableOverhead {
		return true
	}

	return false
}
