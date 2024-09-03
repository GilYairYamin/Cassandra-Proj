import json
from datetime import datetime


# line for push
def find_conflicts(trace_data):
    conflicts = []
    for i in range(len(trace_data) - 1):
        current = trace_data[i]
        next = trace_data[i + 1]

        current_seq = current[0]
        current_query = current[1]
        current_start = (
            datetime.strptime(current[3], "%Y-%m-%d %H:%M:%S.%f")
            if current[3]
            else None
        )
        current_end = datetime.strptime(current[2], "%Y-%m-%d %H:%M:%S.%f")

        next_seq = next[0]
        next_query = next[1]
        next_start = (
            datetime.strptime(next[3], "%Y-%m-%d %H:%M:%S.%f") if next[3] else None
        )

        if current_start and next_start and current_start > next_start:
            conflicts.append(
                {
                    "sequence1": current_seq,
                    "sequence2": next_seq,
                    "query1": current_query,
                    "query2": next_query,
                    "start_time1": current_start.isoformat(),
                    "start_time2": next_start.isoformat(),
                }
            )

    return conflicts


def run_trace_results_conflucts():
    # Read the JSON file
    with open("traceResult.json", "r") as file:
        data = json.load(file)

    # Extract the trace results
    trace_results = data["traceResults"]

    # Find conflicts
    conflicts = find_conflicts(trace_results)

    # Save conflicts to a JSON file
    with open("traceResultConflicts.json", "w") as outfile:
        json.dump({"conflicts": conflicts}, outfile, indent=2)

    # Print summary
    if conflicts:
        print(
            f"Found {len(conflicts)} conflicts. Results saved to traceResultConflicts.json"
        )
    else:
        print("No conflicts found. Empty result saved to traceResultConflicts.json")


if __name__ == "__main__":
    run_trace_results_conflucts()
