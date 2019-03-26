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

#Read configurations from .yaml file
with open(args.config_file_path, "r") as stream:
    configs = yaml.load(stream)

# Print the read configurations
print(configs)

#flink settings
flink_api_address = configs['flink.api.address']

# kafka settings
kafka_address = configs['kafka.server.address']
zookeeper_address = configs['kafka.zookeeper.address']

# source settings
rate_init = int(configs['source.rate.init'])
timer_threads_num = int(configs['source.timer.threads.num'])
key_num = int(configs['source.key.num'])
key_skewness = float(configs['source.key.skewness'])
value_margin = int(configs['source.value.margin'])

#Configure command line
source_command_line = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.KafkaWordGeneratingSource",
    "-b", kafka_address,
    "-k", str(key_num),
    "-m", str(value_margin),
    "-t", str(timer_threads_num),
    "-r", str(rate_init),
    "-w", "uniform"
]

flink_command_line = [
    "flink", "run",
    "window-event-time/target/window-event-time-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", kafka_address,
    "--zookeeper_address", zookeeper_address,
]

sink_command_line = [
    "/home/ubuntu/kafka_2.11-0.11.0.3/bin/kafka-console-consumer.sh",
    "--bootstrap-server",
    kafka_address,
    "--topic",
    "result",
]

#run
source_process=subprocess.Popen(source_command_line)

flink_process=subprocess.Popen(flink_command_line)

time.sleep(5)

#get flink jobs
jobs=requests.get(flink_api_address+"/jobs").json()["jobs"]
job_id_list = []
for job in jobs:
    if job['status'] == 'RUNNING':
        job_id_list.append(job['id'])

#shut down jobs if there's more than 1 job
if len(job_id_list) > 1:
    print("There are %d jobs running. Terminate others before start" % len(job_id_list))
    exit(0)

with open("latency_log.txt", "w") as latency_log_file:
    sink_process = subprocess.Popen(sink_command_line, stdout=latency_log_file)

time.sleep(60)
os.kill(source_process.pid, signal.SIGKILL)
# os.kill(flink_process.pid, signal.SIGKILL)
os.kill(sink_process.pid, signal.SIGKILL)

if source_process is not None:
    os.kill(source_process.pid, signal.SIGKILL)
if sink_process is not None:
    os.kill(sink_process.pid, signal.SIGKILL)

job_id = job_id_list[0]
print("Job ID = %s" % job_id)

requests.patch(flink_api_address + "/jobs/" + job_id)
