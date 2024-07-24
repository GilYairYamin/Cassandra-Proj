import os
import subprocess
import yaml


# Function to read the YAML configuration file
def read_config(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config["cassandra_stress_config"]


normal_parameter = [
    "profile",
    "duration",
    "truncate",
    "cl",
    "send-to",
]


def build_special_parameter(config, param):

    special_param = []
    if param == "ops":
        special_param.append(f'"ops({config["ops"]})"')

    elif param == "node":
        nodes = ",".join(config["node"])
        special_param.append(f"-node {nodes}")

    return special_param


def build_command(config):
    command = [config["command"], config["mode"]]

    for key in config.keys():
        if key == "command" or key == "mode":
            continue
        if key in normal_parameter:
            command.extend([f"{key}={config[key]}"])
        else:
            command.extend(build_special_parameter(config, key))

    return command


# Function to run cassandra-stress with parameters from the configuration file
# def run_cassandra_stress(config):
#     command = [config["command"]]
#     command.append(config["mode"])

#     # Add parameters to the command
#     if "profile" in config:
#         command.extend([f"profile={config['profile']}"])

#     if "ops" in config:
#         command.extend([f"\"ops({config['ops']})\""])

#     if "doration" in config:
#         command.extend([f"duration={config['duration']}"])

#     if "cl" in config:
#         command.extend([f'cl={config["cl"]}'])

#     if "n" in config:
#         command.extend([f'n={str(config["n"])}'])

#     if config.get("no-warmup", False):
#         command.append("-no-warmup")

#     if "pop" in config:
#         for key, value in config["pop"].items():
#             command.extend([f"-pop", f"{key}={value}"])

#     if "nodes" in config:
#         command.extend(["-node", ",".join(config["nodes"])])

#     if "log" in config:
#         command.extend(
#             ["-log", f'file={config["log"]["file"]},level={config["log"]["level"]}']
#         )

#     if "rate" in config:
#         rate = config["rate"]
#         command.extend(
#             [
#                 "-rate",
#             ]
#         )

#         if "threads" in rate:
#             command.extend([f'threads={rate["threads"]}'])

#         if "throttle" in rate:
#             command.extend([f'throttle={rate["throttle"]}'])

#         if "fixed" in rate:
#             command.extend([f'fixed={rate["fixed"]}'])

#         if "auto" in rate.keys() and rate["auto"]:
#             command.extend([f"auto"])

#     if "send-to" in config:
#         command.extend(["-send-to", config["send-to"]])

#     print(command)
#     print(" ".join(command))
#     # Open the log file
#     # with open(config["log"]["file"], "w") as log_file:
#     #     # Run the command
#     #     process = subprocess.Popen(
#     #         command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
#     #     )

#     #     # Read the output and write it to both the terminal and the log file
#     #     for line in iter(process.stdout.readline, ""):
#     #         print(line, end="")  # Print to terminal
#     #         log_file.write(line)  # Write to log file

#     #     # Wait for the process to finish and get the return code
#     #     process.stdout.close()
#     #     return_code = process.wait()

#     # print(f"cassandra-stress ran with return code {return_code}")


if __name__ == "__main__":
    config_file_path = "./cassandra-stress-config.yaml"
    config = read_config(config_file_path)
    command = build_command(config)
    print(" ".join(command))
