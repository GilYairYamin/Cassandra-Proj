import os
import subprocess
import yaml
import os

normal_parameter = [
    "profile",
    "duration",
    "truncate",
    "cl",
    "send-to",
]


def read_config(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config["cassandra_stress_config"]


def build_special_parameter(config, param):
    special_param = ""

    if param == "ops":
        opsConfig = config["ops"]
        opsArr = []
        for key in opsConfig.keys():
            opsArr.append(f"{key}={opsConfig[key]}")
        ops = ",".join(opsArr)
        special_param = f'"ops({ops})"'

    elif param == "node":
        nodes = ",".join(config["node"])
        special_param = f"-node {nodes}"

    return special_param


def build_command(config):
    command = [config["command"], config["mode"]]

    for key in config.keys():
        if key == "command" or key == "mode":
            continue
        if key in normal_parameter:
            command.append(f"{key}={config[key]}")
        else:
            special_param = build_special_parameter(config, key)
            command.append(special_param)

    return command


def init_defaults_to_config(config):
    defaults = {
        "command": "cassandra-stress",
        "mode": "user",
        "n": 10000,
        "node": ["62.90.89.27"],
    }

    for key in defaults:
        if key not in config.keys() or config[key] == None:
            config[key] = defaults[key]


if __name__ == "__main__":
    config_file_path = "./cassandra-stress-config.yaml"
    config = read_config(config_file_path)
    init_defaults_to_config(config)
    command_array = build_command(config)
    command = " ".join(command_array)
    
    os.system(command)
