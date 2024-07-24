import subprocess
import os

# Configuration embedded in the script
config = {
    "executable_path": "/path/to/your/executable",
    "parameters": ["param1", "param2", "param3"]
}

# Function to run the executable with parameters
def run_executable(config):
    executable_path = config.get("executable_path")
    parameters = config.get("parameters", [])

    # Check if the executable exists
    if not os.path.exists(executable_path):
        print(f"Executable not found: {executable_path}")
        return

    # Build the command
    command = [executable_path] + parameters

    # Run the command
    try:
        result = subprocess.run(command, check=True)
        print(f"Executable ran successfully with return code {result.returncode}")
    except subprocess.CalledProcessError as e:
        print(f"Error running executable: {e}")

if __name__ == "__main__":
    run_executable(config)