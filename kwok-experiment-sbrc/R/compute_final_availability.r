#!/usr/bin/env Rscript
library(dplyr)

args = commandArgs(trailingOnly=TRUE)
if (length(args) < 2) {
  stop("Missing arguments. Usage: Rscript compute_final_availability [INPUT_FILE] [OUTPUT_FILE]", call.=FALSE)
}

input_filepath = args[1]
output_filepath = args[2]

#getwd()
#setwd("/local/giovanni/git/test/k8s-qos-driven-plugins/broker")
#input_filepath = "data/20200928_202058_workload-exp2-higher-contention-aggregate-replication4/metrics.log"
#output_filepath = "data/20200928_125856_workload-exp2-medium-contention-aggregate-replication4/final-controllers-qos-new.csv"

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

input$timestamp <- input$timestamp * 1000
input$start <- input$timestamp - (input$runningTime + input$waitingTime + input$allocationTime)

add_final_availability <- function(.data, measuring, final_availability) {
  with_measuring <- .data %>% filter(qosMeasuring == measuring)
  
  if (nrow(with_measuring) == 0) {
    return(with_measuring %>% select(controllerName, qosMeasuring, controllerSlo))
  } 
  
  added_availability <- with_measuring %>%
    group_by(controllerName) %>%
    mutate(replication = max(replicaId) + 1) %>%
    mutate(finalAvailability = final_availability(across(everything()))) %>%
    ungroup
  
  return(added_availability[, c("controllerName", "qosMeasuring", "controllerSlo", "finalAvailability", "replication")] %>% unique)
  #return(added_availability %>% unique)
}
  
# qosMeasuring == "average"
average_availability <- input %>% add_final_availability("average", function(.data) {
  if (nrow(.data) == 0) {
    return(0)
  }
  
  latestAvailability <- .data %>%
    slice(which.max(timestamp)) %>%
    select("controllerAvailability") %>%
    as.numeric
  return(latestAvailability)
})

# qosMeasuring == "independent"
independent_availability <- input %>% add_final_availability("independent", function(.data) {
  if (nrow(.data) == 0) {
    return(0)
  }
  
  lowestAvailability <- .data %>% 
    group_by(replicaId) %>%
    slice(which.max(timestamp)) %>%
    ungroup %>%
    select("controllerAvailability") %>%
    min
  return(lowestAvailability)
})

# qosMeasuring == "concurrent"
concurrent_availability <- input %>% add_final_availability("concurrent", function(.data) {
  if (nrow(.data) == 0) {
    return(0)
  }
  
  latestAvailability <- .data %>%
    slice(which.max(timestamp)) %>%
    select("controllerAvailability") %>%
    as.numeric
  return(latestAvailability)
})

# qosMeasuring == "aggregate"
#g <- input %>% filter(controllerName == "free-job-aggregate-4-88787843d8f1")
aggregate_availability <- input %>% add_final_availability("aggregate", function(.data) {
  if (nrow(.data) == 0) {
    return(0)
  }

  latestAvailabilities <- .data %>%
    group_by(replicaId) %>%
    slice(which.max(timestamp))
  
  print(latestAvailabilities)
  latestAvailabilities <- latestAvailabilities %>% 
    mutate(finalEffectiveRuntime=controllerEffectiveRuntime + runningTime, finalDiscardedRuntime=controllerDiscardedRuntime - runningTime)
  
  #data.frame(latestAvailabilities)
  controllerAvailability <- latestAvailabilities %>% ungroup() %>%
    slice(which.max(finalEffectiveRuntime)) %>%
    mutate(finalControllerAvailability = finalEffectiveRuntime / (finalEffectiveRuntime + finalDiscardedRuntime + controllerWaitingTime + controllerBindingTime)) %>%
    select(finalControllerAvailability) %>%
    as.numeric

  return(min(controllerAvailability, 1.0))
})

final_availability <- bind_rows(average_availability, independent_availability, concurrent_availability, aggregate_availability)
write.csv(final_availability, output_filepath, row.names = FALSE, quote = FALSE)