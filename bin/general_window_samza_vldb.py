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
artificial_window = configs.get('exp.artificial_window', False)

is_event_time = configs.get('eventtime.enabled', False)

if artificial_window and not is_event_time:
    print("eventtime.enabled should be set to True when artificial window is enabled")
    exit(0)

if is_event_time:
    watermark_interval = configs['eventtime.watermark.interval']
    #max_out_of_orderness = configs['eventtime.max_out_of_orderness']

# latency_deadline = int(configs['exp.latency_deadline'])
slope_threshold = 10

query = configs['query']
state_backend = configs['state_backend']


# submit the query firstly to flink
flink_command_line_prefix = [
    "flink", "run",
    "./window-samza-vldb/target/window-samza-vldb-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", kafka_address,
    "--zookeeper_address", zookeeper_address,
    "--query_type", str(query),
    "--state_backend", state_backend,
    "--parallelism", str(parallelism),
    "--is_event_time", str(is_event_time)
]

if query == "session-window":
    flink_command_line_prefix += [
        "--session_gap", str(configs['query.window.session.gap'])
    ]

else:
    flink_command_line_prefix += [
        "--window_size", str(configs['query.window.size']),
        "--window_interval", str(configs['query.window.interval'])
    ]

if state_backend == "rocksdb":
    flink_command_line_prefix += [
        "--rocksdb_path", configs['rocksdb.path'],
        "--block_cache_size", str(configs['rocksdb.block_cache_size']),
        "--write_buffer_size", str(configs['rocksdb.write_buffer_size']),
        "--table_format", str(configs['rocksdb.table_format'])
    ]

elif state_backend == "streamix":
    flink_command_line_prefix += [
        "--state_store_path", configs['streamix.path'],
        "--batch_write_size", str(configs['streamix.batch_write_size']),
        "--batch_read_size", str(configs['streamix.batch_read_size']),
        "--cached_ratio", str(configs['streamix.cached_ratio']),
        "--file_num", str(configs['streamix.file_num'])
    ]

if is_event_time:
    flink_command_line_prefix += [
        "--watermark_interval", str(watermark_interval)
    ]

current_event_rate = rate_init
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

if query == "session-window":
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

java_large_scale_command_line_prefix = [
    "ssh",
    "streamix-w",
    "java",
    "-jar",
    #"-Xms512m",
    #"-Xmx4g",
    "/home/ubuntu/gw-stream-bench/window-largeScale-simul/target/window-largeScale-simul-1.0-SNAPSHOT-shaded.jar",
    "-w", str(configs['exp.artificial_window.size']),
    "-k", str(key_num),
    "-m", str(value_margin),
    "-g", str(configs['streamix.file_num']),
    "-t", str(parallelism),
    "-ast", str(configs['source.session.average_term']),
    "-sg", str(configs['query.window.session.gap']),
    "-i", str(configs['source.session.gap']),
    "-sst", str(configs['streamix.path'])
]

source_process = None

status = "pass"
failure_count = 0
p95_deadline = 25000

try:
    while failure_count < 5:
        if artificial_window:
            java_large_scale_command_line = java_large_scale_command_line_prefix + [
                "-d", str(current_event_rate)
            ]
            try:
                print("\nRun java job on streamix-w\n")
                print(java_large_scale_command_line)
                ssh = subprocess.call(java_large_scale_command_line)
            except:
                print("\nError in java program\n")


        time_diff_flink = None
        time_diff_src = None
        if artificial_window :
            time_diff_flink = int(time.time())*1000
            time_diff_src = time_diff_flink - int(configs['exp.artificial_window.size'])*1000
            print("time_diff_flink: "+str(time_diff_flink))
            print("time_diff_src: "+str(time_diff_src))
        else:
            time_diff_flink = 0
            time_diff_src = 0

        source_command_line = source_command_line_prefix + [
            "-r", str(current_event_rate),
            "-td", str(time_diff_src)
        ]
        flink_command_line = flink_command_line_prefix + [
            "--time_diff", str(time_diff_flink)
        ]


        print("\nStart source process...")
        # Start the source process
        print("Source commandline = %s " % str(source_command_line))
        source_process = subprocess.Popen(source_command_line)

        # Start Flink job
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

        print("Current Thp = %d" % current_event_rate)
        requests.post(slack_webhook_url,
                      json={"text": "Current throughput = %d" % current_event_rate})


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

        if p95latency > p95_deadline:
            status = "fail"
            failure_count += 1 
        else:
            status = "pass"

        requests.post(slack_webhook_url, json={"text": "P50 latency = %d, P95 latency = %d, slope = %f" %
                                                       (p50latency, p95latency, slope)})
        print("P50 latency = %d, P95 latency = %d, slope = %f" % (p50latency, p95latency, slope))

        if status == "fail":
            requests.post(slack_webhook_url,
                          json={"text": "Eval *failed* at thp = %d T.T" % current_event_rate}) 
        else:
            requests.post(slack_webhook_url,
                          json={"text": "Eval *passed* at thp = %d :)" % current_event_rate}) 
            failure_count = 0

        current_event_rate += rate_increase

        print("Killing the flink job...")
        requests.patch(flink_api_address + "/jobs/" + job_id)


except:
    print("Killing the source process and the flink job...")
    if source_process is not None:
        os.kill(source_process.pid, signal.SIGKILL)
    #requests.patch(flink_api_address + "/jobs/" + job_id)
    print("Evaluation Interrupted!")
    requests.post(slack_webhook_url,
                  json={"text": "Evaluation interrupted!"})
    raise

print("Evaluation finished.")
