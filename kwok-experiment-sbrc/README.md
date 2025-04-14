
# Project Name

This repository contains scripts and tools for emulating nodes and pods using KWOK, collecting metrics with Prometheus, and automating trace collection for experiments. These tools provide an efficient way to simulate and measure Kubernetes workloads.

## Prerequisites

Before running the scripts, ensure you have the following installed:

1. A container runtime like **Docker** (required for running dependencies).

   To install Docker, follow the instructions [here](https://docs.docker.com/get-docker/). Alternatively, you can use other container runtimes supported by KWOK, such as `containerd`.

## Quick Start

### Running the Experiment

1. Clone this repository:

   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. Run the `run_all.sh` script with the desired **experiment duration** and **step collection**:

   ```bash
   ./run_all.sh <experiment_duration> <step_collection>
   ```

   Replace `<experiment_duration>` and `<step_collection>` with your desired values. For example:
   - `3600` for an hour-long experiment.
   - `5s` for a 5-second step collection interval.

### Script Workflow

The `run_all.sh` script automates the following steps:

1. **Dependency Installation**:
   - Installs and sets up **Prometheus** and **KWOK**, along with their dependencies. If these are already installed, the script will skip this step.

2. **Node and Pod Creation**:
   - Configures the nodes and pods to be emulated using KWOK.

3. **Prometheus Port Forwarding**:
   - Sets up port forwarding to access Prometheus metrics locally.

4. **Experiment Monitoring**:
   - Monitors the experiment and checks if the specified duration has been met.

5. **Cluster Cleanup**:
   - Deletes the nodes and pods created for the experiment.

6. **Trace Collection**:
   - Runs a Python script to collect traces and metrics from the experiment.

### Outputs

The metrics and traces collected during the experiment are saved in the `output` directory for further analysis.

## Notes

- Ensure your system has sufficient resources to emulate the specified number of nodes and pods.
- Root privileges might be required to run some commands, depending on your environment.
- Prometheus and KWOK logs can be checked for troubleshooting if issues arise.

## Troubleshooting

If you encounter any issues:

- Verify that Docker or another container runtime is installed and running correctly.
- Check the logs of the `run_all.sh` script for detailed error messages.
- Make sure Prometheus and KWOK dependencies are installed and properly configured.

For further assistance, please open an issue in this repository.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
