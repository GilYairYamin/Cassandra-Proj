import json
import re
import sys
#line for push
def extract_queries_and_timestamps(data, timestamp_line):
    queries_and_times = data['queries_and_times']
    timestamps = []
    
    # Pattern to extract the name and toTS from the query
    insert_pattern = re.compile(r"INSERT INTO simpletry.person \(id, name, toTS\) VALUES \('[^']+', '([^']+)', \{'(\d+)':toTimestamp\(now\(\)\)\}\);")
    update_pattern = re.compile(r"UPDATE simpletry.person SET name = '([^']+)', toTS\['(\d+)'\] = toTimestamp\(now\(\)\) WHERE id = '[^']+';")
    
    for query, _ in queries_and_times:
        insert_match = insert_pattern.search(query)
        update_match = update_pattern.search(query)
        if insert_match:
            name = insert_match.group(1)
            toTS = int(insert_match.group(2))
        elif update_match:
            name = update_match.group(1)
            toTS = int(update_match.group(2))
        else:
            print(f"Warning: No name or toTS found in query: {query}")
            continue
        
        trace_timestamps = [
            trace[timestamp_line] for trace in data['traces'] 
            if trace['query'] == query
        ]
        if trace_timestamps:
            timestamps.append((name, toTS, trace_timestamps[0]))
        else:
            print(f"Warning: No timestamp found for query: {query}")
    
    return timestamps

def find_conflicts(timestamps):
    conflicts = []
    
    # Sort the queries by the numeric part of their names
    sorted_queries = sorted(timestamps, key=lambda x: int(re.search(r'\d+', x[0]).group()))
    
    # Check for conflicts
    for i in range(len(sorted_queries) - 1):
        current_name, current_toTS, current_timestamp = sorted_queries[i]
        next_name, next_toTS, next_timestamp = sorted_queries[i + 1]
        
        if current_timestamp > next_timestamp:
            conflict = {
                'earlier_name': current_name,
                'earlier_toTS': current_toTS,
                'earlier_timestamp': current_timestamp,
                'later_name': next_name,
                'later_toTS': next_toTS,
                'later_timestamp': next_timestamp
            }
            conflicts.append(conflict)
    
    return conflicts

def save_data(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_json_file> <timestamp_line>")
        sys.exit(1)

    input_filename = sys.argv[1]
    timestamp_line = sys.argv[2]
    extracted_filename = 'extracted_data.json'
    conflicts_filename = 'conflicts.json'
    
    try:
        with open(input_filename, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print(f"Error: File '{input_filename}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: File '{input_filename}' is not a valid JSON file.")
        sys.exit(1)
    
    timestamps = extract_queries_and_timestamps(data, timestamp_line)
    print(f"Extracted data: {timestamps}")
    save_data(timestamps, extracted_filename)
    
    conflicts = find_conflicts(timestamps)
    print(f"Conflicts: {conflicts}")
    save_data(conflicts, conflicts_filename)
    
    print(f"Extracted data saved to {extracted_filename}")
    print(f"Conflicts saved to {conflicts_filename}")