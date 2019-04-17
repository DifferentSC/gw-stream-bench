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

#flink settings
flink_api_address = configs['flink.api.address']

# kafka settings
kafka_address = configs['kafka.server.address']
zookeeper_address = configs['kafka.zookeeper.address']

#source settings
rate_init = int(configs['source.rate.init'])
rate_increase = int(configs['source.rate.increase'])
timer_threads_num = int(configs['source.timer.threads.num'])
key_num = int(configs['source.key.num'])
key_skewness = float(configs['source.key.skewness'])
value_margin = int(configs['source.value.margin'])

query = configs['query']
state_backend = configs['state_backend']
#event time settings
streamix_time = configs['streamix.time']
watermark_interval = configs['streamix.watermark_interval']
max_timelag = configs['streamix.max_timelag']

#session gap
session_gap = configs['query.window.session.gap']

#exp settings
time_wait = int(configs['exp.wait_time'])
parallelism = int(configs['exp.parallelism'])
time_running = int(configs['exp.running_time'])


source_command_line = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.KafkaWordGeneratingSource",
    "-b", kafka_address,
    "-k", str(key_num),
    "-s", str(key_skewness),
    "-m", str(value_margin),
    "-t", str(timer_threads_num),
    "-w", "uniform-session",
    "-ast", str(configs['source.session.average_term']),
    "-sg", str(configs['source.session.gap']),
    "-r", str(rate_init)
]
source_process = None

#start source process
source_process = subprocess.Popen(source_command_line)
print("Started source process...")
time.sleep(5)

flink_command_line = [
    "flink", "run",
    "./window-event-time/target/window-event-time-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", kafka_address,
    "--zookeeper_address", zookeeper_address,
    "--query_type", str(query),
    "--state_backend", state_backend,
    "--parallelism", str(parallelism),
    "--watermark_interval", str(watermark_interval),
    "--max_timelag", str(max_timelag),
    "--session_gap", str(session_gap),
    "--state_store_path", configs['streamix.path'],
    "--batch_write_size", str(configs['streamix.batch_write_size']),
    "--batch_read_size", str(configs['streamix.batch_read_size']),
    "--cached_ratio", str(configs['streamix.cached_ratio']),
    "--file_num", str(configs['streamix.file_num'])
]

#start flink process
submit_guery = subprocess.Popen(flink_command_line)
print("Started flink process...")
time.sleep(5)

jobs = requests.get(flink_api_address + "/jobs").json()["jobs"]
job_id_list = []
for job in jobs:
    if job['status'] == 'RUNNING':
        print("job running!")


start_time = time.time()
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

# Kill the source process
os.kill(source_process.pid, signal.SIGKILL)
source_process = None
# Kill the sink process
os.kill(sink_process.pid, signal.SIGKILL)
sink_process = None
# Kill the flink process
os.kill(submit_query.pid, signal.SIGKILL)
submit_query = None

