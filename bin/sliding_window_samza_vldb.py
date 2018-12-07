import argparse
import yaml
import subprocess
import time
import os, signal
import requests

configs = None

parser = argparse.ArgumentParser()
parser.add_argument("config_file_path", type=str, help="The path of configuration file ending with .yml")
args = parser.parse_args()

with open(args.config_file_path, "r") as stream:
    configs = yaml.load(stream)

# Print the read configurations
print(configs)

flink_api_address = configs['flink.api.address']
flink_backpressure_threshold = configs['flink.backpressure.threshold']

kafka_address = configs['kafka.server.address']
zookeeper_address = configs['kafka.zookeeper.address']
key_num = int(configs['key.num'])
key_skewness = float(configs['key.skewness'])
value_margin = int(configs['value.margin'])
window_size = int(configs['window.size'])
window_interval = int(configs['window.interval'])
query = configs['query']

rate_init = int(configs['rate.init'])
rate_increase = int(configs['rate.increase'])
timer_threads_num = int(configs['timer.threads.num'])

time_wait = int(configs['time.wait'])
time_running = int(configs['time.running'])
deadline_latency = int(configs['deadline.latency'])
logging = bool(configs['logging'])

state_backend = configs['state_backend']

# submit the query firstly to flink
flink_common_command_line = [
    "flink", "run",
    "./window-samza-vldb/target/window-samza-vldb-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", kafka_address,
    "--zookeeper_address", zookeeper_address,
    "--window_size", str(window_size),
    "--window_interval", str(window_interval),
    "--query_type", str(query),
    "--state_backend", state_backend
]

flink_command_line = None

if state_backend == "mem":
    flink_command_line = flink_common_command_line

elif state_backend == "rocksdb":
    flink_command_line = flink_common_command_line + [
        "--rocksdb_path", configs['rocksdb.path'],
        "--block_cache_size", str(configs['rocksdb.block_cache_size']),
        "--write_buffer_size", str(configs['rocksdb.write_buffer_size']),
        "--table_format", str(configs['rocksdb.table_format'])
    ]

elif state_backend == "streamix":
    flink_command_line = flink_common_command_line + [
        "--state_store_path", configs['streamix.path'],
        "--batch_write_size", str(configs['streamix.batch_write_size']),
        "--file_num", str(configs['streamix.file_num'])
    ]

print("Submit the query the flink")
# Submit the query to flink
submit_query = subprocess.Popen(flink_command_line)
time.sleep(5)

jobs = requests.get(flink_api_address + "/jobs").json()["jobs"]
job_id_list = []
for job in jobs:
    if job['status'] == 'RUNNING':
        job_id_list.append(job['id'])

if len(job_id_list) > 1:
    print("There are %d jobs running. Terminate others before start" % len(job_id_list))
    exit(0)

job_id = job_id_list[0]

print("Job ID = %s" % job_id)

vertices = requests.get(flink_api_address + "/jobs/" + job_id).json()['vertices']
vertices_id_list = []
for vertex in vertices:
    vertices_id_list.append(vertex['id'])

print("Vertices ID = %s" % vertices_id_list)

current_event_rate = rate_init - rate_increase
success = True

source_command_line_prefix = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.KafkaWordGeneratingSource",
    "-b", kafka_address,
    "-k", str(key_num),
    "-s", str(key_skewness),
    "-m", str(value_margin),
    "-t", str(timer_threads_num)
]

sink_command_line = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.sink.KafkaLatencyMeasure",
    "-b", kafka_address,
    "-t", str(time_running),
    "-d", str(deadline_latency),
    "-l", str(logging)
]

source_process = None

# Monitor backpressure to initiate sampling
backpressure_map = {}
end_timestamp_map = {}

for vertex_id in vertices_id_list:
    backpressure = requests.get(flink_api_address +
                                "/jobs/" + job_id + "/vertices/" + vertex_id + "/backpressure").json()
    while backpressure['status'] == 'deprecated':
        print("Sleep for 5 seconds to get backpressure samples...")
        time.sleep(5)
        backpressure = requests.get(flink_api_address +
                                    "/jobs/" + job_id + "/vertices/" + vertex_id + "/backpressure").json()
    backpressure_map[vertex_id] = []
    end_timestamp_map[vertex_id] = 0

success = True

try:
    while success:
        current_event_rate += rate_increase
        print("Current Thp = %d" % current_event_rate)
        source_command_line = source_command_line_prefix + [
            "-r", str(current_event_rate)
        ]
        print("Start source process...")
        # Start the source process
        source_process = subprocess.Popen(source_command_line)
        # Wait for the designated time
        print("Waiting for %d secs..." % time_wait)
        time.sleep(time_wait)
        # Start the sink process
        print("Measure latency for %d secs..." % time_running)
        # sink_process = subprocess.call(sink_command_line)

        start_time = time.time()
        current_backpressure_timestamp = 0

        while time.time() - start_time < time_running:
            success = True
            for vertex_id in vertices_id_list:
                backpressure = requests.get(flink_api_address +
                                            "/jobs/" + job_id + "/vertices/" + vertex_id + "/backpressure").json()
                if backpressure['end-timestamp'] > end_timestamp_map[vertex_id]:
                    backpressure_map[vertex_id].append(backpressure)
                    print("Vertex %s: Backpressure-level = %s" % (vertex_id, backpressure['backpressure-level']))
                    end_timestamp_map[vertex_id] = backpressure['end-timestamp']
            time.sleep(5)

        success = True
        for vertex_id in vertices_id_list:
            high_backpressure_count = 0
            for backpressure in backpressure_map[vertex_id]:
                if backpressure['backpressure-level'] == 'high':
                    high_backpressure_count += 1
            if high_backpressure_count > len(backpressure_map[vertex_id]) * flink_backpressure_threshold:
                success = False

        # Kill the source process
        os.kill(source_process.pid, signal.SIGKILL)
        source_process = None

        """
        # Collect the result
        with open("result.txt", "r") as result_stream:
            result = result_stream.readline().strip()
            success = (result == "success")
        """



except:
    print("Killing the source process and the flink job...")
    if source_process is not None:
        os.kill(source_process.pid, signal.SIGKILL)
    requests.patch(flink_api_address + "/jobs/" + job_id)
    print("Evaluation Interrupted!")
    raise

print("Killing the flink job...")
requests.patch(flink_api_address + "/jobs/" + job_id)
print("Evaluation finished.")
