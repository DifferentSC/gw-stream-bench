#!/usr/bin/env python2
import argparse
import yaml
import subprocess
import time
import os, signal
import requests
import numpy as np

configs = None

parser = argparse.ArgumentParser()
parser.add_argument("config_file_path", type=str, help="The path of configuration file ending with .yml")
parser.add_argument("nexmark_query", type=str, help="Nexmark Query")
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
parallelism = int(configs['exp.parallelism'])
# latency_deadline = int(configs['exp.latency_deadline'])
slope_threshold = 10

query = configs['query']
state_backend = configs['state_backend']

# submit the query firstly to flink
flink_command_line = [
    "flink", "run",
    "./nexmark/target/nexmark-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", kafka_address,
    "--zookeeper_address", zookeeper_address,
    "--nexmark_query", args.nexmark_query,
    "--state_backend", state_backend,
    "--parallelism", str(parallelism)
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
        "--batch_read_size", str(configs['streamix.batch_read_size']),
        "--cached_ratio", str(configs['streamix.cached_ratio']),
        "--file_num", str(configs['streamix.file_num'])
    ]

print("Submit the query the flink. Command line = " + str(flink_command_line))
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

current_event_rate = rate_init
success = True

source_command_line_prefix = [
    "java", "-jar",
    "./nexmark-generator/target/nexmark-generator-1.0-SNAPSHOT-shaded.jar",
    "--kafka_broker_address", kafka_address
]

source_process = None

# Monitor backpressure to initiate sampling
backpressure_map = {}

for vertex_id in vertices_id_list:
    backpressure = requests.get(flink_api_address +
                                "/jobs/" + job_id + "/vertices/" + vertex_id + "/backpressure").json()
    while backpressure['status'] == 'deprecated':
        print("Sleep for 5 seconds to get backpressure samples...")
        time.sleep(5)
        backpressure = requests.get(flink_api_address +
                                    "/jobs/" + job_id + "/vertices/" + vertex_id + "/backpressure").json()
    backpressure_map[vertex_id] = []

status = "pass"
fail_count = 0

try:
    while status != "fail":
        print("Current Thp = %d" % current_event_rate)
        requests.post(slack_webhook_url,
                      json={"text": "Current throughput = %d" % current_event_rate})
        source_command_line = source_command_line_prefix + [
            "--events_per_sec", str(current_event_rate)
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
        # current_backpressure_timestamp = 0

        with open("latency_log.txt", "w") as latency_log_file:
            sink_command_line = [
                "/home/ubuntu/kafka_2.11-0.11.0.3/bin/kafka-console-consumer.sh",
                "--bootstrap-server",
                kafka_address,
                "--topic",
                "result",
            ]
            sink_process = subprocess.Popen(sink_command_line, stdout=latency_log_file)
            while time.time() - start_time < time_running:
                time.sleep(1)

        """
        backpressure_num = 0
        while time.time() - start_time < time_running:
            backpressure_num += 1
            for vertex_id in vertices_id_list:
                backpressure = requests.get(flink_api_address +
                                            "/jobs/" + job_id + "/vertices/" + vertex_id + "/backpressure").json()
                backpressure_map[vertex_id].append(backpressure)
                print("Vertex %s: Backpressure-level = %s" % (vertex_id, backpressure['backpressure-level']))
            time.sleep(2.5)

        total_backpressure_list = ["ok"] * backpressure_num

        # success = True
        for vertex_id in vertices_id_list:
            high_backpressure_count = 0
            index = 0
            for backpressure in backpressure_map[vertex_id]:
                if backpressure['backpressure-level'] == 'high':
                    total_backpressure_list[index] = 'high'
                elif backpressure['backpressure-level'] == 'low':
                    if total_backpressure_list[index] == 'ok':
                        total_backpressure_list[index] = 'low'
                index += 1

            # Initialize the backpressure map
            backpressure_map[vertex_id] = []

        is_backpressure_high_list = map(lambda x: x == 'high', total_backpressure_list)
        if reduce(lambda x, y: x and y, is_backpressure_high_list):
            status = "fail"
        elif is_backpressure_high_list[backpressure_num - 1]:
            status = "hold"
        else:
            status = "pass"
        """

        # Kill the source process
        os.kill(source_process.pid, signal.SIGKILL)
        source_process = None
        # Kill the sink process
        os.kill(sink_process.pid, signal.SIGKILL)
        sink_process = None

        latency_list = []
        with open("latency_log.txt", "r") as latency_log_file:
            for line in latency_log_file:
                value_string = line.strip()
                try:
                    latency = int(value_string)
                    latency_list.append(latency)
                except ValueError:
                    pass

        if len(latency_list) == 0:
            print("Cannot read latencies...!")
            raise ValueError

        # Determine continuous increasing pattern with linear regression
        time_step = float(time_running) / float(len(latency_list))
        current_time = 0
        times = []
        for i in range(0, len(latency_list)):
            times.append(current_time)
            current_time += time_step

        slope, y_intercept = np.linalg.lstsq(
            np.vstack([np.array(times), np.ones(len(times))]).T,
            latency_list)[0]
        latency_list.sort()

        p50latency = latency_list[int(len(latency_list) * 0.5)]
        p95latency = latency_list[int(len(latency_list) * 0.95)]

        if slope > slope_threshold and fail_count < 2:
            status = "hold"
        elif slope > slope_threshold and fail_count >= 2:
            status = "fail"
        else:
            status = "pass"

        requests.post(slack_webhook_url, json={"text": "P50 latency = %d, P95 latency = %d, slope = %f" %
                                                       (p50latency, p95latency, slope)})
        print("P50 latency = %d, P95 latency = %d, slope = %f" % (p50latency, p95latency, slope))

        if status == "fail":
            requests.post(slack_webhook_url,
                          json={"text": "Eval *failed* at thp = %d T.T" % current_event_rate})
        elif status == "hold":
            requests.post(slack_webhook_url,
                          json={"text": "Eval *held* at thp = %d :|" % current_event_rate})
            fail_count += 1
        else:
            requests.post(slack_webhook_url,
                          json={"text": "Eval *passed* at thp = %d :)" % current_event_rate})
            current_event_rate += rate_increase
            fail_count = 0

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