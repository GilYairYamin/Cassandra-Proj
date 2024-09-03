from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
import json
import datetime

# Lists to store the results
traces_res = []
queries_and_times = []


def execute_command(command, session):
    # Set consistency level for the query
    consistency_level = ConsistencyLevel.ONE
    query = SimpleStatement(command, consistency_level=consistency_level)

    # Execute the query with tracing enabled
    res = session.execute(query, trace=True)

    try:
        # Retrieve query trace
        trace = res.get_query_trace(max_wait_sec=1)

        # Initialize variables to capture timestamps
        coordinator_timestamp = None
        memtable_timestamp = None
        events_list = []

        # Process each event in the trace
        for event in trace.events:
            # Save event details
            events = {
                "description": event.description,
                "source": event.source,
                "source_elapsed": event.source_elapsed,
                "thread_name": event.thread_name,
                "datetime": event.datetime.isoformat(),
            }
            events_list.append(events)

            # Capture timestamps for coordinator and memtable events
            if event.description in (
                "Enqueuing response to /",
                "Adding to memtable",
                "Adding to person memtable",
            ):
                memtable_timestamp = event.datetime.isoformat()
            if event.description in (
                "Determining replicas for mutation",
                "Parsing",
                "Preparing statement",
            ):
                coordinator_timestamp = event.datetime.isoformat()

        # Create a dictionary to store the trace details
        data = {
            "trace_id": trace.trace_id,
            "request_type": trace.request_type,
            "client": trace.client,
            "coordinator": trace.coordinator,
            "started_at": trace.started_at.isoformat(),
            "parameters": trace.parameters,
            "duration": str(trace.duration),
            "query": trace.parameters["query"],
            "events": events_list,
            "coordinator_timestamp": coordinator_timestamp,
            "memtable_timestamp": memtable_timestamp,
        }

        # Print the captured timestamps
        if memtable_timestamp:
            print(f"Command '{command}' reached memtable at {memtable_timestamp}")
        if coordinator_timestamp:
            print(f"Command '{command}' reached coordinator at {coordinator_timestamp}")

        # Append the trace data to the results list
        traces_res.append(data)

        # Add the command and its execution timestamp to the queries_and_times list
        queries_and_times.append([command, datetime.datetime.now().isoformat()])

    except Exception as e:
        print("Error retrieving query trace:", e)
    return res


def run_workload_from_file(filename, session):
    # Read commands from the file
    with open(filename, "r") as file:
        commands = file.readlines()

    # Execute each command
    for command in commands:
        execute_command(command.strip(), session)


def run_trace_simple():
    # Authentication details for the Cassandra cluster
    auth_provider = PlainTextAuthProvider(username="omrino", password="sfgs44Df")

    # Connect to the Cassandra cluster
    cluster = Cluster(
        contact_points=["62.90.89.27", "62.90.89.28", "62.90.89.29", "62.90.89.39"],
        auth_provider=auth_provider,
    )

    session = cluster.connect("simpletry")
    session.default_consistency_level = ConsistencyLevel.ONE

    # Run workload commands from the file
    run_workload_from_file("workload_commands.txt", session)

    # Create the final data structure with organized sections
    final_data = {"queries_and_times": queries_and_times, "traces": []}

    # Process each trace result and organize the data
    for trace in traces_res:
        organized_trace = {
            "query": trace["query"],
            "launched_at": trace["started_at"],
            "coordinator_timestamp": trace.get("coordinator_timestamp"),
            "memtable_timestamp": trace.get("memtable_timestamp"),
            "details": trace["events"],
        }
        final_data["traces"].append(organized_trace)

    # Save the results to a JSON file
    with open("result.json", "w") as file:
        json.dump(
            final_data, file, ensure_ascii=False, sort_keys=True, default=str, indent=4
        )

    # Shutdown the session and cluster connection
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    run_trace_simple()
