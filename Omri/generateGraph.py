import json
import matplotlib.pyplot as plt
from datetime import datetime

def create_enhanced_conflict_marked_graph(extracted_data_path, conflicts_data_path, output_image_path):
    """
    Creates a scatter plot of Index Number vs Time, marking conflicting indices with a different color.
    Enhancements:
    1. Removes the background grid.
    2. Removes the top and right spines.
    3. Adds a legend on the top left side.
    
    Parameters:
    - extracted_data_path: Path to the 'extracted_data.json' file.
    - conflicts_data_path: Path to the 'conflicts.json' file.
    - output_image_path: Path where the output image will be saved.
    """
    
    # Load extracted data from the JSON file
    with open(extracted_data_path, 'r') as file:
        data = json.load(file)
    
    # Load conflict data from the JSON file
    with open(conflicts_data_path, 'r') as file:
        conflicts = json.load(file)
    
    # Extract 'earlier_toTS' values to identify conflicting indices
    conflict_indices_set = {entry["earlier_toTS"] for entry in conflicts}
    
    # Prepare separate lists for normal and conflict points
    normal_times = []
    normal_indices = []
    conflict_times = []
    conflict_indices = []
    
    for entry in data:
        index = entry[1]
        timestamp_str = entry[2]
        
        # Skip entries with null timestamps
        if timestamp_str is None:
            continue
        
        # Convert timestamp string to datetime object
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # Handle cases where microseconds might be missing
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
        
        # Categorize the data point as normal or conflict
        if index in conflict_indices_set:
            conflict_times.append(timestamp)
            conflict_indices.append(index)
        else:
            normal_times.append(timestamp)
            normal_indices.append(index)
    
    # Create the plot
    plt.figure(figsize=(14, 8))
    
    # Plot normal points
    plt.scatter(normal_times, normal_indices, color='blue', label='Normal')
    
    # Plot conflict points
    plt.scatter(conflict_times, conflict_indices, color='red', label='Conflict')
    
    # Labeling the axes and title
    plt.xlabel('Time')
    plt.ylabel('Index Number')
    plt.title('Index Number vs Time with Conflict Markings')
    
    # Remove the background grid for a cleaner look
    # (No need to call plt.grid())
    
    # Adjust the X-axis labels for better readability
    plt.gcf().autofmt_xdate()
    plt.xticks(rotation=45)
    
    # Remove the top and right spines
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Add a legend on the top left side
    plt.legend(loc='upper left')
    
    # Optionally, adjust layout to prevent clipping of labels
    plt.tight_layout()
    
    # Save the graph as an image
    plt.savefig(output_image_path, format='png')
    plt.close()

# Usage example
create_enhanced_conflict_marked_graph(
    'extracted_data.json',
    'conflicts.json',
    'enhanced_conflict_marked_index_vs_time.png'
)
