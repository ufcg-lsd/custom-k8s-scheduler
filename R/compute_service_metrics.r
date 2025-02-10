library(dplyr)
#library("Rmisc")
#library(ggplot2)

args = commandArgs(trailingOnly=TRUE)
if (length(args) < 1) {
  stop("Missing arguments. Usage: Rscript compute_service_metrics [EXPERIMENT_DIR]", call.=FALSE)
}

load_latencies <- function(latency_file) {
  latencies <- read.csv(latency_file)
  colnames(latencies) = c("latency")
  #colnames(latencies) = c("timestamp", "latency")
  latencies
}

experiment_dir = args[1]
#experiment_dir <- paste0("../exp-results/exp3-defining-service-rate/", "20210319_212643_workload-define-service-rate", "/")

print(paste0("Reading latencies of production applications on ", experiment_dir))

latency_files <- list.files(experiment_dir, pattern = "^prod", full.names = TRUE)
#latency_files <- list.files(experiment_dir, pattern = "^prod-\\.latency$", full.names = TRUE)

all_latencies_qos <- data.frame()

latency_file <- latency_files[1]

for(latency_file in latency_files) {
  current_deploy <- strsplit(latency_file, ".latency")[[1]]
  current_deploy_id <- basename(current_deploy)
  
  print(paste0("Reading ", latency_file))
  df_current_latencies <- load_latencies(latency_file) 
  df_current_latencies <- df_current_latencies %>% dplyr::mutate(index = seq.int(nrow(df_current_latencies)), deploy_id = current_deploy_id)

  all_latencies_qos <- rbind(all_latencies_qos, data.frame(df_current_latencies))
}  

df_prod_data <- all_latencies_qos %>% dplyr::summarise(minimum=min(latency), mean=mean(latency), maximum=max(latency), total=n(), q50 = quantile(latency, 0.5), q75 = quantile(latency, 0.75), q90 = quantile(latency, 0.90), q95 = quantile(latency, 0.95), q99 = quantile(latency, 0.99))

write.csv(df_prod_data, paste0(experiment_dir, "v2-service-metrics-prod.csv"), row.names = TRUE, quote = FALSE)

df_prod_data_per_deploy <- all_latencies_qos %>% group_by(deploy_id) %>%
  dplyr::summarise(minimum=min(latency), mean=mean(latency), maximum=max(latency), total=n(), q50 = quantile(latency, 0.5), q75 = quantile(latency, 0.75), q90 = quantile(latency, 0.90), q95 = quantile(latency, 0.95), q99 = quantile(latency, 0.99))

write.csv(df_prod_data_per_deploy, paste0(experiment_dir, "service-metrics-prod-per-deploy.csv"), row.names = TRUE, quote = FALSE)

prod_q95 <- df_prod_data$q95[1]
prod_q90 <- df_prod_data$q90[1]

batch_threshold_q95 <- prod_q95 + prod_q95 * 10/100
batch_threshold_q90 <- prod_q90 + prod_q90 * 10/100

print(paste0("Reading latencies of batch applications on ", experiment_dir))

latency_files <- list.files(experiment_dir, pattern = "^batch", full.names = TRUE)

latency_file <- latency_files[1]

all_batch_latencies <- data.frame()

for(latency_file in latency_files) {
  current_deploy <- strsplit(latency_file, ".latency")[[1]]
  current_deploy_id <- basename(current_deploy)
  
  print(paste0("Reading ", latency_file))
  df_current_latencies <- load_latencies(latency_file) 
  df_current_latencies <- df_current_latencies %>% dplyr::mutate(index = seq.int(nrow(df_current_latencies)), deploy_id = current_deploy_id)
  
  all_batch_latencies <- rbind(all_batch_latencies, data.frame(df_current_latencies))
}  

df_batch_data <- all_batch_latencies %>% dplyr::summarise(minimum=min(latency), mean=mean(latency), maximum=max(latency), total=n(), q50 = quantile(latency, 0.5), q75 = quantile(latency, 0.75), q90 = quantile(latency, 0.90), q95 = quantile(latency, 0.95), q99 = quantile(latency, 0.99))

threshold_percentil_q95 <- ecdf(all_batch_latencies$latency)(batch_threshold_q95)
threshold_percentil_q90 <- ecdf(all_batch_latencies$latency)(batch_threshold_q90)

df_batch_data <- df_batch_data %>% dplyr::mutate(threshold_quantile_q95 = threshold_percentil_q95, threshold_quantile_q90 = threshold_percentil_q90)

write.csv(df_batch_data, paste0(experiment_dir, "v2-service-metrics-batch.csv"), row.names = TRUE, quote = FALSE)

# per deploy

df_batch_data_per_deploy <- all_batch_latencies %>% group_by(deploy_id) %>%
  dplyr::summarise(minimum=min(latency), mean=mean(latency), maximum=max(latency), total=n(), q50 = quantile(latency, 0.5), q75 = quantile(latency, 0.75), q90 = quantile(latency, 0.90), q95 = quantile(latency, 0.95), q99 = quantile(latency, 0.99), threshold_quantile_q95 = ecdf(latency)(batch_threshold_q95), threshold_quantile_q90 = ecdf(latency)(batch_threshold_q90))

write.csv(df_batch_data_per_deploy, paste0(experiment_dir, "service-metrics-batch-per-deploy.csv"), row.names = TRUE, quote = FALSE)