#!/usr/bin/env Rscript
require(dplyr)

args = commandArgs(trailingOnly=TRUE)
if (length(args) < 2) {
  stop("Missing arguments. Usage: Rscript final_availability [INPUT_FILE] [OUTPUT_FILE]", call.=FALSE)
}

input_filepath = args[1]
output_filepath = args[2]


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

add_final_availability <- function(.data, measuring, final_availability) {
  with_measuring <- .data %>% filter(qosMeasuring == measuring)
  added_availability <- with_measuring %>%
    group_by(controllerName) %>%
    mutate(finalAvailability = final_availability(across(everything()))) %>%
    ungroup
  return(added_availability[, c("controllerName", "qosMeasuring", "controllerSlo", "finalAvailability")] %>% unique)
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
aggregate_availability <- input %>% add_final_availability("aggregate", function(.data) {
  if (nrow(.data) == 0) {
    return(0)
  }
  
  times <- .data %>%
    select("runningTime", "waitingTime", "allocationTime") %>%
    colSums
  
  runtime <- times["runningTime"] %>%
    sum
  
  effectiveRuntime <- .data %>%
    filter(terminationStatus == "true") %>%
    select("runningTime") %>%
    sum
  
  discardedRuntime <- runtime - effectiveRuntime
  
  shouldBeEffective <- .data %>%
    group_by(replicaId) %>%
    slice(which.max(timestamp)) %>%
    ungroup %>%
    select("runningTime") %>%
    sum
  
  return((effectiveRuntime + shouldBeEffective) / sum(times))
})

final_availability <- bind_rows(average_availability, independent_availability, concurrent_availability, aggregate_availability)
write.csv(final_availability, output_filepath, row.names = FALSE, quote = FALSE)