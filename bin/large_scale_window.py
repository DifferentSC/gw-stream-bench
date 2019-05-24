import argparse
import yaml
import requests
import subprocess
import time

configs = None

parser = argparse.ArgumentParser()
parser.add_argument("config_file_path", type=str, help="The path of configuration file ending with .yml")
args = parser.parse_args()

with open(args.config_file_path, "r") as stream:
    configs = yaml.load(stream)

# Print the read configurations
print(configs)
"""
slack_webhook_url = 'https://hooks.slack.com/services/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'

requests.post(slack_webhook_url,
              json={"text": str(configs)})
"""

# flink settings
flink_api_address = configs['flink.api.address']

window_size = str(configs['window.size'])
key_num = str(configs['num.keys'])
value_margin = str(configs['margin.size'])
group_num = str(configs['group.num'])
parallelism = str(configs['num.threads'])
average_session_term = str(configs['average.session.term'])
session_gap = str(configs['session.gap'])
inactive_time = str(configs['inactive.time'])
state_store_path = str(configs['state.store.path'])
data_rate = str(configs['data.rate'])

"""
flink_large_scale_command_line = [
    "flink", "run",
    "./window-largeScale-simul/target/window-largeScale-simul-1.0-SNAPSHOT-shaded.jar",
    "--window_size", window_size,
    "--num_keys", num_keys,
    "--margin_size", margin_size,
    "--group_num", group_num,
    "--num_threads", num_threads,
    "--average_session_term", average_session_term,
    "--session_gap", session_gap,
    "--inactive_time", inactive_time,
    "--state_store_path", state_store_path,
    "--data_rate", data_rate
]
"""

java_large_scale_command_line = [
    "java", 
    "-jar",
    "-Xms512m",
    "-Xmx4g",
    "-Xloggc:gc_memory_logs.log",
    "-XX:+PrintGCDetails",
    "-XX:+PrintGCTimeStamps",
    "-XX:NewRatio=2",
    "-XX:-UseAdaptiveSizePolicy",
    "./window-largeScale-simul/target/window-largeScale-simul-1.0-SNAPSHOT-shaded.jar",
    "-w", window_size,
    "-k", key_num,
    "-m", value_margin,
    "-g", group_num,
    "-t", parallelism,
    "-ast", average_session_term,
    "-sg", session_gap,
    "-i", inactive_time,
    "-sst", state_store_path,
    "-d", data_rate
]


print("Java job. Command line = "+str(java_large_scale_command_line)+"\n")
submit_query = subprocess.call(java_large_scale_command_line)
#time.sleep(300)

"""
#check whether flink job is running
print("\nChecking jobs...")
jobs = requests.get(flink_api_address + "/jobs").json()["jobs"]
print("after request get\n");
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

#kill flink job
print("Killing the flink job...")
requests.patch(flink_api_address + "/jobs/" + job_id)
print("Evaluation finished.")

"""

