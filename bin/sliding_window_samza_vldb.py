import argparse
import yaml
import subprocess
import time
import os, signal

configs = None

parser = argparse.ArgumentParser()
parser.add_argument("config_file_path", type=str, help="The path of configuration file ending with .yml")
args = parser.parse_args()

with open(args.config_file_path, "r") as stream:
    configs = yaml.load(stream)


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
        "--block_cache_size", configs['rocksdb.block_cache_size'],
        "--write_buffer_size", configs['rocksdb.write_buffer_size']
    ]

elif state_backend == "streamix":
    flink_command_line = flink_common_command_line + [
        "--state_store_path", configs['streamix.path'],
        "--batch_write_size", configs['streamix.batch_write_size']
    ]

# Submit the query to flink
submit_query = subprocess.call(flink_command_line)

current_event_rate = rate_init - rate_increase
success = True

source_command_line_prefix = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.KafkaWordGeneratingSource",
    "-b", kafka_address,
    "-k", str(key_num),
    "-s", str(key_skewness),
    "-m", str(value_margin),
    "-t", str(timer_threads_num)
]

sink_command_line = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.KafkaLatencyMeasure",
    "-b", kafka_address,
    "-t", time_running,
    "-d", deadline_latency
]

source_process = None

try:
    while success:
        current_event_rate += rate_increase
        source_command_line = source_command_line_prefix + [
            "-r", str(current_event_rate)
        ]
        # Start the source process
        source_process = subprocess.Popen(source_command_line)
        # Wait for the designated time
        time.sleep(time_wait)
        # Start the sink process
        sink_process = subprocess.call(sink_command_line)
        # Kill the source process
        os.kill(source_process.pid, signal.SIGKILL)
        source_process = None
        # Collect the result
        with open("result.txt", "r") as result_stream:
            result = result_stream.readline()
            success = (result == "success")
        print("Current Thp = %d" % current_event_rate)

except KeyboardInterrupt:
    if source_process is not None:
        print("Killing the source process...")
        os.kill(source_process.pid, signal.SIGKILL)
    print("Evaluation Interrupted!")

print("Evaluation finished.")
