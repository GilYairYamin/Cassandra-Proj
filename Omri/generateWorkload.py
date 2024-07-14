from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
import json
import random
import datetime

workload_commands = []

def generate_workload(num_inserts=10, num_updates=5, num_deletes=5, mix_commands=False):
    global workload_commands
    person_id = 208306068
    current_number = 0

    # Generate insert commands
    for i in range(1, num_inserts + 1):
        person_name = f'omri{i}'
        time_stamp_id = f'{i}'
        command = f"INSERT INTO simpletry.person (id, name, toTS) VALUES ('{person_id}', '{person_name}', {{'{time_stamp_id}':toTimestamp(now())}});"
        workload_commands.append(command)
        current_number = i

    # Generate update commands starting from the next number
    for i in range(1, num_updates + 1):
        person_name = f'Aviv/Gil{current_number + i}'
        time_stamp_id = f'{current_number + i}'
        command = f"UPDATE simpletry.person SET name = '{person_name}', toTS['{time_stamp_id}'] = toTimestamp(now()) WHERE id = '{person_id}';"
        workload_commands.append(command)

    current_number += num_updates

    # Generate delete commands starting from the next number
    for i in range(1, num_deletes + 1):
        time_stamp_id = f'{current_number + i}'
        command = f"DELETE toTS['{time_stamp_id}'] FROM simpletry.person WHERE id = '{person_id}';"
        workload_commands.append(command)

    # Mix commands if the option is enabled
    if mix_commands:
        random.shuffle(workload_commands)

def save_workload_to_file(filename):
    with open(filename, 'w') as file:
        for command in workload_commands:
            file.write(command + '\n')

if __name__ == "__main__":
    auth_provider = PlainTextAuthProvider(username='omrino', password='sfgs44Df')
    cluster = Cluster(contact_points=['62.90.89.27', '62.90.89.28', '62.90.89.29', '62.90.89.39'], auth_provider=auth_provider)

    session = cluster.connect('simpletry')
    session.default_consistency_level = ConsistencyLevel.ONE

    session.execute("CREATE KEYSPACE IF NOT EXISTS simpletry WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };")

    session.execute('DROP TABLE IF EXISTS person;')

    session.execute("CREATE TABLE IF NOT EXISTS person (id text, name text, toTS MAP<text,timestamp>, PRIMARY KEY (id));")

    # Generate workload commands with specified numbers and mix option
    generate_workload(num_inserts=50, num_updates=30, num_deletes=0, mix_commands=False)

    # Save workload commands to a file
    save_workload_to_file('workload_commands.txt')

    session.shutdown()
    cluster.shutdown()
