# Cassandra Workload Application

## Overview

This repository contains a Python application designed to execute a series of INSERT and UPDATE commands on a Cassandra database. The application uses multiple threads to perform these operations and provides detailed timing and conflict analysis.

## Getting Started

To use the code, please follow these steps:

1. Clone the GitHub repository.
2. Switch to the `gils-changes` branch.
3. Download the `cassandraApp.py` file located inside the `full-app` directory.

## What It Does

1. **Generates a series of INSERT and UPDATE commands**.
2. **Executes these commands** on a Cassandra database using multiple threads.
3. **Collects detailed timing information and traces** for each operation.
4. **Detects and analyzes potential conflicts** in write operations.
5. **Produces visual and textual outputs** for analysis.

## Key Functions

- `generate_workload()`: Creates the workload commands based on user input.
- `execute_command()`: Runs a single Cassandra command and gathers trace data.
- `run_workload()`: Manages the entire workload execution process.
- `find_conflicts()`: Identifies potential conflicts in the executed operations.
- `create_enhanced_conflict_marked_graph()`: Generates a visual representation of the workload execution.

## How to Run

1. SSH to the cluster using one of the IP addresses of the cluster.
   
2. Insert the following lines into the command line:

    ```bash
    source ./venv/bin/activate
    python cassandraApp.py
    ```

3. Follow the prompts to input:
   - Number of INSERT operations
   - Number of UPDATE operations
   - Number of threads to use
   - Whether to use TIMESTAMP clause for INSERTs

   The application will execute the workload and generate result files in a timestamped directory.

4. **Optional**: To run the stress test simultaneously, run `execute-cassandra-stress.py`. This script will run for a minute, so make sure to run your actual code within that timeframe.

## Output

- Workload commands file
- Detailed results JSON file
- Conflicts JSON file
- Graph image showing operation timings and conflicts
