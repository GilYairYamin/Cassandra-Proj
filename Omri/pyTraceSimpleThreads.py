import threading
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
import json
import datetime
from queue import Queue

# Lists to store the results
traces_res = []
queries_and_times = []

# Lock for appending results to shared lists
lock = threading.Lock()


def execute_command(command, session):
    consistency_level = ConsistencyLevel.ONE
    query = SimpleStatement(command, consistency_level=consistency_level)

    res = session.execute(query, trace=True)

    try:
        trace = res.get_query_trace(max_wait_sec=10)
        coordinator_timestamp = None
        memtable_timestamp = None
        events_list = []

        for event in trace.events:
            events = {
                "description": event.description,
                "source": event.source,
                "source_elapsed": event.source_elapsed,
                "thread_name": event.thread_name,
                "datetime": event.datetime.isoformat(),
            }
            events_list.append(events)

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

        if memtable_timestamp:
            print(f"Command '{command}' reached memtable at {memtable_timestamp}")
        if coordinator_timestamp:
            print(f"Command '{command}' reached coordinator at {coordinator_timestamp}")

        with lock:
            traces_res.append(data)
        with lock:
            queries_and_times.append([command, datetime.datetime.now().isoformat()])

    except Exception as e:
        print("Error retrieving query trace:", e)
    return res


def worker(session, command_queue):
    while not command_queue.empty():
        command = command_queue.get()
        if command is None:
            break
        execute_command(command, session)
        command_queue.task_done()


def run_workload_from_file(filename, session, num_threads):
    with open(filename, "r") as file:
        commands = [command.strip() for command in file.readlines()]

    command_queue = Queue()
    for command in commands:
        command_queue.put(command)

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(session, command_queue))
        t.start()
        threads.append(t)

    command_queue.join()

    for t in threads:
        t.join()


if __name__ == "__main__":
    auth_provider = PlainTextAuthProvider(username="omrino", password="sfgs44Df")
    cluster = Cluster(
        contact_points=["62.90.89.27", "62.90.89.28", "62.90.89.29", "62.90.89.39"],
        auth_provider=auth_provider,
    )
    session = cluster.connect("simpletry")
    session.default_consistency_level = ConsistencyLevel.ONE

    # Specify the number of threads you want to use
    num_threads = 10  # Adjust this number as needed

    # Run workload commands from the file using the specified number of threads
    run_workload_from_file("workload_commands.txt", session, num_threads)

    final_data = {"queries_and_times": queries_and_times, "traces": []}

    for trace in traces_res:
        organized_trace = {
            "query": trace["query"],
            "launched_at": trace["started_at"],
            "coordinator_timestamp": trace.get("coordinator_timestamp"),
            "memtable_timestamp": trace.get("memtable_timestamp"),
            "details": trace["events"],
        }
        final_data["traces"].append(organized_trace)

    with open("result.json", "w") as file:
        json.dump(
            final_data, file, ensure_ascii=False, sort_keys=True, default=str, indent=4
        )

    session.shutdown()
    cluster.shutdown()
