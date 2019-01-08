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

slack_webhook_url = 'https://hooks.slack.com/services/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'

requests.post(slack_webhook_url,
              json={"text": str(configs)})

# flink settings
flink_api_address = configs['flink.api.address']

# kafka settings
kafka_address = configs['kafka.server.address']
zookeeper_address = configs['kafka.zookeeper.address']

# source settings
rate_init = int(configs['source.rate.init'])
rate_increase = int(configs['source.rate.increase'])
timer_threads_num = int(configs['source.timer.threads.num'])
key_num = int(configs['source.key.num'])
key_skewness = float(configs['source.key.skewness'])
value_margin = int(configs['source.value.margin'])

# exp settings
time_wait = int(configs['exp.wait_time'])
time_running = int(configs['exp.running_time'])
backpressure_threshold = float(configs['exp.backpressure_threshold'])

query = configs['query']
state_backend = configs['state_backend']

# submit the query firstly to flink
flink_command_line = [
    "flink", "run",
    "./window-samza-vldb/target/window-samza-vldb-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", kafka_address,
    "--zookeeper_address", zookeeper_address,
    "--query_type", str(query),
    "--state_backend", state_backend
]

if query == "session-window":
    flink_command_line += [
        "--session_gap", str(configs['query.window.session.gap'])
    ]

else:
    flink_command_line += [
        "--window_size", str(configs['query.window.size']),
        "--window_interval", str(configs['query.window.interval'])
    ]

if state_backend == "rocksdb":
    flink_command_line += [
        "--rocksdb_path", configs['rocksdb.path'],
        "--block_cache_size", str(configs['rocksdb.block_cache_size']),
        "--write_buffer_size", str(configs['rocksdb.write_buffer_size']),
        "--table_format", str(configs['rocksdb.table_format'])
    ]

elif state_backend == "streamix":
    flink_command_line += [
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

if query == "session":
    source_command_line_prefix += [
        "-w", "uniform-session",
        "-ast", str(configs['source.session.average_term']),
        "-sg", str(configs['source.session.gap'])
    ]

else:
    source_command_line_prefix += [
        "-w", "uniform"
    ]

"""
sink_command_line = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.sink.KafkaLatencyMeasure",
    "-b", kafka_address,
    "-t", str(time_running),
    "-d", str(deadline_latency),
    "-l", str(logging)
]
"""

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
        requests.post(slack_webhook_url,
                      json={"text": "Current throughput = %d" % current_event_rate})
        source_command_line = source_command_line_prefix + [
            "-r", str(current_event_rate)
        ]
        print("Start source process...")
        # Start the source process
        print("Source commandline = %s " % str(source_command_line))
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
            if high_backpressure_count == len(backpressure_map[vertex_id]):
                success = False
                print("high backpressure count: %d, which is bigger than %d"
                      % (high_backpressure_count, len(backpressure_map[vertex_id]) * backpressure_threshold))
            # Initialize the backpressure & timestamp map
            backpressure_map[vertex_id] = []
            end_timestamp_map[vertex_id] = 0

        # Kill the source process
        os.kill(source_process.pid, signal.SIGKILL)
        source_process = None

        if success:
            requests.post(slack_webhook_url,
                          json={"text": "Eval *passed* at thp = %d :)" % current_event_rate})
        else:
            requests.post(slack_webhook_url,
                          json={"text": "Eval *failed* at thp = %d T.T" % current_event_rate})

except:
    print("Killing the source process and the flink job...")
    if source_process is not None:
        os.kill(source_process.pid, signal.SIGKILL)
    requests.patch(flink_api_address + "/jobs/" + job_id)
    print("Evaluation Interrupted!")
    requests.post(slack_webhook_url,
                  json={"text": "Evaluation interrupted!"})
    raise

print("Killing the flink job...")
requests.patch(flink_api_address + "/jobs/" + job_id)
print("Evaluation finished.")
