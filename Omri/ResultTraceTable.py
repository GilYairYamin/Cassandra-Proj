import json
from datetime import datetime


# The generated matrix (stored in traceResult.json) is a 2D table where each row represents a command and its associated timestamps. The columns are as follows:

# 1. Sequence Number: The sequence number of the command as per its position in the workload_commands.txt file.
# 2. Command: The actual command that was executed.
# 3. Memtable Timestamp: The timestamp when the command reached the memtable. This value can be None if the command did not reach the memtable.
# 4. Coordinator Timestamp: The timestamp when the command reached the coordinator. This value can be None if the command did not reach the coordinator.


def process_trace_results(input_file, output_file):
    # Read the results from the JSON file
    with open(input_file, 'r') as file:
        data = json.load(file)
    
    # Initialize a list to store the 2D table
    result_table = []

    # Create a dictionary to map commands to sequence numbers
    command_sequence = {command.strip(): i+1 for i, command in enumerate(open('workload_commands.txt').readlines())}

    # Initialize a set to keep track of which sequence numbers are captured
    captured_sequences = set()

    # Iterate through the traces to find the commands that reached the memtable and coordinator
    for trace in data["traces"]:
        memtable_timestamp = trace.get("memtable_timestamp")
        coordinator_timestamp = trace.get("coordinator_timestamp")
        if memtable_timestamp or coordinator_timestamp:
            command = trace["query"]
            sequence_number = command_sequence.get(command)
            if sequence_number:
                memtable_datetime = datetime.fromisoformat(memtable_timestamp) if memtable_timestamp else None
                coordinator_datetime = datetime.fromisoformat(coordinator_timestamp) if coordinator_timestamp else None
                result_table.append([sequence_number, command, memtable_datetime, coordinator_datetime])
                captured_sequences.add(sequence_number)

    # Sort the result table by sequence number (first column)
    result_table.sort(key=lambda x: x[0])

    # Log which sequence numbers were not captured
    missing_sequences = set(range(1, len(command_sequence) + 1)) - captured_sequences
    print(f"Missing sequence numbers: {sorted(missing_sequences)}")

    # Create the output data structure
    output_data = {
        "traceResults": result_table
    }

    # Save the results to the output JSON file
    with open(output_file, 'w') as file:
        json.dump(output_data, file, ensure_ascii=False, indent=4, default=str)

if __name__ == "__main__":
    # Define the input and output file paths
    input_file = 'result.json'
    output_file = 'traceResult.json'

    # Process the trace results and generate the 2D table
    process_trace_results(input_file, output_file)
