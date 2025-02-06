package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"

	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"

	vegeta "github.com/tsenart/vegeta/lib"
)

func GetKubeClient() *kubernetes.Clientset {

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("HOME")+"/.kube/config")
	if err != nil {
		panic(err.Error())
	}

	// creates the clientset
	kubeclient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return kubeclient
}

func tokenGenerator() string {
	b := make([]byte, 6)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func dump(info, file string) {

	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	check(err)
	defer f.Close()

	w := bufio.NewWriter(f)

	w.WriteString(info)
	w.Flush()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var (
	wg        sync.WaitGroup
	layout    = "2006-01-02 15:04:05 +0000 UTC"
	clientset = GetKubeClient()
)

func main() {
	log.Println("Iniciando execução do programa...")
	start := time.Now()
	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) != 4 {
		fmt.Printf("Invalid number of arguments.\n The expected arguments are <workload_file> <experiment_duration> <service_attack_rate> <number_to_factorial>\n")
		os.Exit(1)
	}

	workloadFile := string(argsWithoutProg[0])
	fmt.Printf("Workload File: %s\n", workloadFile)

	experimentDuration, _ := strconv.Atoi(argsWithoutProg[1])
	fmt.Printf("Experiment Duration (seconds): %d\n", experimentDuration)

	serviceAttackRate, _ := strconv.Atoi(argsWithoutProg[2])
	fmt.Printf("Service Attack rate (per instance): %d\n", serviceAttackRate)

	factorialNumber, _ := strconv.Atoi(argsWithoutProg[3])
	fmt.Printf("Factorial number to be computed: %d\n", factorialNumber)

	// executing experiment
	var timeRef = 0
	file, err := ioutil.ReadFile(workloadFile)
	log.Printf("Conteúdo do arquivo: %s", file)

	if err == nil {

		r := csv.NewReader(strings.NewReader(string(file)))

		for {
			record, err := r.Read()

			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			if string(record[0]) != "jid" {

				timestamp, _ := strconv.Atoi(string(record[0]))
				slo := string(record[11])
				cpuReq := string(record[8])
				memReq := string(record[9])
				taskID := string(record[1])
				class := string(record[10])

				deployDuration, _ := strconv.Atoi(string(record[6]))
				controllerKind := string(record[12])
				replicaOrParallelism := string(record[13])
				completions := string(record[14])
				qosMeasuring := string(record[15])

				controllerName := class + "-" + controllerKind + "-" + qosMeasuring + "-" + taskID + "-" + tokenGenerator()

				fmt.Printf("[CONTROLLER INFO] timestamp %d | controllerName %s | slo %s | cpuReq %s | memReq %s | taskId %s | class %s | controllerKind %s "+
					"| replication %s | completions %s | qosMeasuring %s \n", timestamp, controllerName, slo, cpuReq, memReq, taskID, class, controllerKind,
					replicaOrParallelism, completions, qosMeasuring)

				replicaOrParallelismInt, _ := strconv.Atoi(replicaOrParallelism)
				replicaOrParallelismInt32 := int32(replicaOrParallelismInt)

				completionsInt, _ := strconv.Atoi(completions)
				completionsInt32 := int32(completionsInt)

				if controllerKind == "deployment" {
					nodePort, _ := strconv.Atoi(string(record[16]))
					deployment := getDeploymentSpec(controllerName, cpuReq, memReq, slo, class, &replicaOrParallelismInt32, qosMeasuring)

					service := getServiceOfDeployment(deployment, int32(nodePort))
					fmt.Println("Reading new Deployment ...")
					fmt.Printf("Deployment %s, cpu: %v, mem: %v, slo: %s, replicas: %s, qosMeasuring: %s, acceptableOverhead: %s, nodePort %d\n", controllerName,
						cpuReq, memReq, slo, replicaOrParallelism, qosMeasuring, "0.05", nodePort)

					fmt.Printf("Creating service %s-service\n", controllerName)
					_, err = clientset.CoreV1().Services("default").Create(context.TODO(), service, metav1.CreateOptions{})
					if err != nil {
						fmt.Printf("[ERROR] While creating service: \n", err)
					}

					if timestamp == timeRef {
						fmt.Println("Time: ", timestamp)
						fmt.Println("Creating deployment ", controllerName)
						wg.Add(1)
						go CreateAndManageDeploymentTermination(controllerName, deployment, service, min(experimentDuration-timeRef, deployDuration), &wg)
					} else {
						waittime := int(timestamp - timeRef)
						timeRef = timestamp
						fmt.Println("")
						time.Sleep(time.Duration(waittime) * time.Second)
						fmt.Println("Time: ", timestamp)
						fmt.Println("Creating deployment ", controllerName)
						wg.Add(1)
						go CreateAndManageDeploymentTermination(controllerName, deployment, service, min(experimentDuration-timeRef, deployDuration), &wg)
					}

					wg.Add(1)
					go CreateAndManageDemand(controllerName, nodePort, serviceAttackRate, replicaOrParallelismInt, min(experimentDuration-timeRef, deployDuration), factorialNumber, &wg)

				} else if controllerKind == "job" {
					taskDuration := string(record[17])
					job := getJobSpec(controllerName, cpuReq, memReq, slo, class, &replicaOrParallelismInt32, &completionsInt32, qosMeasuring, taskDuration)

					fmt.Println("Reading new Job ...")
					fmt.Printf("Job %s, cpu: %v, mem: %v, slo: %s, paralelism: %d, completions: %d, qosMeasuring: %s, acceptableOverhead: %s, taskDuration %s\n", controllerName,
						cpuReq, memReq, slo, replicaOrParallelism, completions, qosMeasuring, "0.05", taskDuration)

					if timestamp == timeRef {
						fmt.Println("Time: ", timestamp)
						fmt.Println("Creating job ", controllerName)
						wg.Add(1)
						go CreateAndManageJobTermination(controllerName, job, experimentDuration-timeRef, &wg)

					} else {
						waittime := int(timestamp - timeRef)
						timeRef = timestamp
						fmt.Println("")
						time.Sleep(time.Duration(waittime) * time.Second)
						fmt.Println("Time: ", timestamp)
						fmt.Println("Creating job ", controllerName)
						wg.Add(1)
						go CreateAndManageJobTermination(controllerName, job, experimentDuration-timeRef, &wg)
					}
				}
			}
		}
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Println("Finished - runtime: ", elapsed)
}

func CreateAndManageDeploymentTermination(controllerName string, deployment *appsv1.Deployment, service *apiv1.Service,
	expectedRuntime int, wg *sync.WaitGroup) {

	fmt.Printf("Managing deployment %s with %d seconds as expected runtime .\n", controllerName, expectedRuntime)

	_, errCreation := clientset.AppsV1().Deployments("default").Create(context.TODO(), deployment, metav1.CreateOptions{})

	if errCreation != nil {
		fmt.Printf("[ERROR] While creating deployment: ", errCreation)
	}

	// period while the deployment is exist in the cluster
	time.Sleep(time.Duration(expectedRuntime) * time.Second)

	fmt.Printf("Killing deployment %s and its respective service after %d seconds.\n", controllerName, expectedRuntime)

	errDeployDeletion := clientset.AppsV1().Deployments("default").Delete(context.TODO(), controllerName, metav1.DeleteOptions{})
	if errDeployDeletion != nil {
		fmt.Printf("[ERROR] While deleting deployment: ", errDeployDeletion)
	}

	errServiceDeletion := clientset.CoreV1().Services("default").Delete(context.TODO(), service.Name, metav1.DeleteOptions{})
	if errServiceDeletion != nil {
		fmt.Printf("[ERROR] While deleting service: \n", errServiceDeletion)
	}

	wg.Done()
}

func CreateAndManageDemand(controllerName string, nodePort int, frequency int, numberOfReplicas int, expectedRuntime int, factorialNumber int, wg *sync.WaitGroup) {

	// creates the service demand 5 seconds after the deployment creation
	//time.Sleep(time.Duration(30) * time.Second)
	time.Sleep(time.Duration(120) * time.Second)

	// stops the service demand 2 minutes before the deployment is terminated
	freq := frequency * numberOfReplicas
	duration := time.Duration(expectedRuntime-120) * time.Second

	fmt.Printf("Creating the demmand to service on port %d during %d seconds (expectedRuntime was %d).\n", nodePort, duration, expectedRuntime)

	rate := vegeta.Rate{Freq: freq, Per: time.Second}
	targeter := vegeta.NewStaticTargeter(vegeta.Target{
		Method: "GET",
		URL:    "http://localhost:" + strconv.Itoa(nodePort) + "/factorial.php" + "?number=" + strconv.Itoa(factorialNumber),
	})

	attacker := vegeta.NewAttacker(vegeta.Timeout(2*time.Minute), vegeta.Connections(1000))

	var metrics vegeta.Metrics
	var out string
	var latencies = []float64{}
	//var latencies = []string{}

	for res := range attacker.Attack(targeter, rate, duration, fmt.Sprintf("app %s on port %d\n", controllerName, nodePort)) {
		metrics.Add(res)
		latencies = append(latencies, res.Latency.Seconds())
		//latencyLine := fmt.Sprintf("%d,%f", res.Timestamp.Unix(), res.Latency.Seconds())
		//latencies = append(latencies, latencyLine)
	}
	metrics.Close()

	fmt.Printf("Finishing attack on port: %s. Dumping the latencies... \n", strconv.Itoa(nodePort))

	for _, value := range latencies {
		out += fmt.Sprintf("%f\n", value)
	}
	dump(out, os.Getenv("HOME")+"/"+controllerName+".latency")
	//	dump(out, os.Getenv("HOME") + "/" + controllerName + ".new.latency")

	fmt.Printf("Service running on port: %s\n", strconv.Itoa(nodePort))
	fmt.Printf("Mean: %s\n", metrics.Latencies.Mean)
	fmt.Printf("50th percentile: %s\n", metrics.Latencies.P50)
	fmt.Printf("95th percentile: %s\n", metrics.Latencies.P95)
	fmt.Printf("99th percentile: %s\n", metrics.Latencies.P99)
	fmt.Printf("Max percentile: %s\n", metrics.Latencies.Max)
	fmt.Printf("Requests: %v\n", metrics.Requests)
	fmt.Printf("Success: %f\n", metrics.Success)
	fmt.Printf("Throughtput: %f\n", metrics.Throughput)

	out = fmt.Sprintf("%s,%f,%f,%f,%f,%f,%v,%f,%f\n", controllerName, metrics.Latencies.Mean.Seconds(),
		metrics.Latencies.P50.Seconds(), metrics.Latencies.P95.Seconds(), metrics.Latencies.P99.Seconds(),
		metrics.Latencies.Max.Seconds(), metrics.Requests, metrics.Success, metrics.Throughput)

	fmt.Printf("Service running on port: %s - %s\n", strconv.Itoa(nodePort), out)

	dump(out, os.Getenv("HOME")+"/serviceapplication.dat")
	wg.Done()
}

func CreateAndManageJobTermination(controllerName string, job *batchv1.Job, expectedRuntime int, wg *sync.WaitGroup) {
	_, errCreation := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
	if errCreation != nil {
		fmt.Printf("[ERROR] Error while creating job information: ", errCreation)
	}

	time.Sleep(time.Duration(expectedRuntime) * time.Second)

	fmt.Printf("Killing job %s after %d seconds.\n", controllerName, expectedRuntime)

	// The use of clientset to delete the job does not work as expected. The job was deleted, but the Completed
	// pods of the job continue being listed as pods of default namespace
	cmd := exec.Command("/usr/bin/kubectl", "delete", "job", controllerName)
	cmd.Run()

	wg.Done()
}

func getDeploymentSpec(controllerRefName string,
	cpuReq string, memReq string, slo string, class string,
	replicaOrParallelism *int32, qosMeasuring string) *appsv1.Deployment {

	memReqFloat, _ := strconv.ParseFloat(memReq, 64)
	memReqKi := memReqFloat * 1000000
	memReqStr := strconv.FormatFloat(memReqKi, 'f', -1, 64)
	memRequest := memReqStr + "Ki"
	fmt.Println(memRequest)
	rl := apiv1.ResourceList{apiv1.ResourceName(apiv1.ResourceMemory): resource.MustParse(memRequest),
		apiv1.ResourceName(apiv1.ResourceCPU): resource.MustParse(cpuReq)}

	priorityClass := class + "-priority"
	gracePeriod := int64(0)

	// TODO We should make acceptable overhead configurable
	podTemplate := apiv1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"app": "nginx", "controller": controllerRefName},
			Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qosMeasuring": qosMeasuring, "acceptablePreemptionOverhead": "0.05"},
		},
		Spec: apiv1.PodSpec{
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers: []apiv1.Container{{
				Name:  controllerRefName,
				Image: "viniciusbds/app:v0.0.2",
				Resources: apiv1.ResourceRequirements{
					Limits:   rl,
					Requests: rl,
				},
			}},
			PriorityClassName: priorityClass,
			SchedulerName:     "custom-scheduler",
		},
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: controllerRefName},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: podTemplate.Labels},
			Replicas: replicaOrParallelism,
			Template: podTemplate},
	}

	return deployment
}

func getJobSpec(controllerRefName string,
	cpuReq string, memReq string, slo string, class string,
	replicaOrParallelism *int32, completions *int32, qosMeasuring string, taskDuration string) *batchv1.Job {

	memReqFloat, _ := strconv.ParseFloat(memReq, 64)
	memReqKi := memReqFloat * 1000000
	memReqStr := strconv.FormatFloat(memReqKi, 'f', -1, 64)
	memRequest := memReqStr + "Ki"
	fmt.Println(memRequest)
	rl := apiv1.ResourceList{apiv1.ResourceName(apiv1.ResourceMemory): resource.MustParse(memRequest),
		apiv1.ResourceName(apiv1.ResourceCPU): resource.MustParse(cpuReq)}

	priorityClass := class + "-priority"
	gracePeriod := int64(0)

	// TODO We should make acceptable overhead configurable
	podSpec := apiv1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"app": "nginx"},
			Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qosMeasuring": qosMeasuring, "acceptablePreemptionOverhead": "0.05"},
		},
		Spec: apiv1.PodSpec{
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers: []apiv1.Container{{
				Name:    controllerRefName,
				Image:   "nginx:1.15",
				Command: []string{"sleep", taskDuration},
				Resources: apiv1.ResourceRequirements{
					Limits:   rl,
					Requests: rl,
				},
			}},
			PriorityClassName: priorityClass,
			RestartPolicy:     "Never",
			SchedulerName:     "custom-scheduler",
		},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: controllerRefName, Namespace: "default"},

		Spec: batchv1.JobSpec{
			Template:    podSpec,
			Parallelism: replicaOrParallelism,
			Completions: completions,
		},
	}

	job.Spec.Template.Labels = map[string]string{"app": "nginx"}

	return job
}

func getServiceOfDeployment(deployment *appsv1.Deployment, nodePort int32) *apiv1.Service {
	servicePort := apiv1.ServicePort{Port: 80, NodePort: nodePort}
	serviceSpec := apiv1.ServiceSpec{
		Ports:    []apiv1.ServicePort{servicePort},
		Selector: deployment.Spec.Template.Labels,
		Type:     apiv1.ServiceTypeNodePort,
	}

	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: deployment.Name + "-service", Namespace: "default"},
		Spec:       serviceSpec}
	return service
}
