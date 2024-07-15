### Instructions for Running the Workload

1. **Generate Workload**
   - Execute `generateWorkload.py` to create a text file containing Cassandra CQL commands.

2. **Run Workload**
   - Execute `pyTraceSimple.py` or `pyTraceSimpleThreads.py` to run the generated workload. This process will also produce a JSON file named `result.json` with a comprehensive description of the results.

3. **Generate Result Table**
   - Execute `ResultTraceTable.py` to create a table with timestamps and commands based on `result.json`.
