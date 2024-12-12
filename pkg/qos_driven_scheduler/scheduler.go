package qos_driven_scheduler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const PluginName = "QosDrivenScheduler"

// QosDrivenScheduler implementa as interfaces de plugins solicitadas
type QosDrivenScheduler struct{}

// Garantindo que QosDrivenScheduler implementa as interfaces necessárias
var _ framework.QueueSortPlugin = &QosDrivenScheduler{}
var _ framework.ReservePlugin = &QosDrivenScheduler{}
var _ framework.PreBindPlugin = &QosDrivenScheduler{}
var _ framework.PostBindPlugin = &QosDrivenScheduler{}
var _ framework.PostFilterPlugin = &QosDrivenScheduler{}

// Name retorna o nome do plugin
func (scheduler *QosDrivenScheduler) Name() string {
	return PluginName
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
func (scheduler *QosDrivenScheduler) PreBind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	klog.Infof("[PreBind] Validando o pod %s antes de vincular ao node %s", pod.Name, nodeName)
	return framework.NewStatus(framework.Success)
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

// Função de inicialização do plugin
func New() func(ctx context.Context, args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return func(ctx context.Context, args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
		klog.Infof("Plugin %s inicializado com sucesso", PluginName)
		return &QosDrivenScheduler{}, nil
	}
}
