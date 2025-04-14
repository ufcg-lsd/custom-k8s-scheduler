#!/usr/bin/env Rscript
library(dplyr)

args = commandArgs(trailingOnly=TRUE)
if (length(args) < 2) {
  stop("Missing arguments. Usage: Rscript compute_final_availability [INPUT_FILE] [OUTPUT_FILE]", call.=FALSE)
}

input_filepath = args[1]
#workload_filepath = args[2]
output_filepath = args[2]

#input_filepath = "../exp-results/exp2-more-pods/30s/20201008_020247_workload-exp2-no-contention-aggregate-replication4-v2/metrics.log"
#workload_filepath = "../broker/workloads/workload-exp2-no-contention-aggregate-replication4-v2.csv"
#output_filepath = "~/GolandProjects/k8s-qos-driven-plugins/exp-results/exp2-more-pods/30s/20201008_020247_workload-exp2-no-contention-aggregate-replication4-v2/makespan.csv"

input <- read.csv(input_filepath, header=FALSE)
colnames(input) <- c("timestamp",
                     "podName",
                     "controllerName", 
                     "controllerSlo",
                     "replicaId",
                     "qosMeasuring", 
                     "creationTime",
                     "waitingTime",
                     "allocationTime", 
                     "runningTime",
                     "terminationStatus",
                     "controllerAvailability",
                     "qosMetric",
                     "controllerWaitingTime",
                     "controllerBindingTime",
                     "controllerEffectiveRuntime",
                     "controllerDiscardedRuntime")

#head(input)

#workload <- read.csv(workload_filepath, header=FALSE)
#colnames(workload) <- c("submit_time", "job_id", "task_id", "user", "scheduling_class",
#                             "priority", "service_time", "end_time", "cpu_req", "mem_req",
#                             "slo_class", "slo", "controller", "total_replicas", "completions", "qos_measuring", "port", "task_duration")

start <- input %>%
  group_by(controllerName) %>%
  summarise(firstCreationTime = min(creationTime))

end <- input %>%
  filter(as.integer(terminationStatus) == 2) %>%
  group_by(controllerName) %>%
  summarise(lastSuccess = max(timestamp))

successes <- input %>%
  filter(as.integer(terminationStatus) == 2) %>%
  count(controllerName) %>%
  rename(succeededTerminations = n)

failures <- input %>%
  filter(as.integer(terminationStatus) == 1) %>%
  count(controllerName) %>%
  rename(nonSucceededTerminations = n)

#read the workload file the expected success jobs

output <- left_join(start, end, 
                    by = c("controllerName" = "controllerName"))
output <- left_join(output, successes, 
                    by = c("controllerName" = "controllerName"))
output <- left_join(output, failures, 
                    by = c("controllerName" = "controllerName"))

# checl if success == expected and compute the makespam

output <- output %>% dplyr::mutate(makespan = ifelse(!is.na(lastSuccess), lastSuccess - firstCreationTime, NA))
#output$makespan = output$lastSuccess - output$firstCreationTime

output <- output %>% select("controllerName", "succeededTerminations", "nonSucceededTerminations", "makespan")

write.csv(output, output_filepath, row.names = FALSE, quote = FALSE)