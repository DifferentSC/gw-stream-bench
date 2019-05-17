import argparse
import yaml
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
window_size = configs['window_size']
num_keys = configs['num_keys']
margin_size = configs['margin_size']
group_num = configs['group_num']
num_threads = configs['num_threads']
data_rate = configs['data_rate']
average_session_term = configs['average_session_term']
session_gap = configs['session_gap']
inactive_time = configs['inactive_time']
state_store_path = configs['state_store_path']

flink_large_scale_command_line = [
    "flink", "run",
    "./window-largeScale-simul/target/window-largeScale-simul-1.0-SNAPSHOT-shaded.jar",
    "--window_size", window_size,
    "--num_keys", num_keys,
    "--margin_size", margin_size,
    "--group_num", group_num,
    "--num_threads", num_threads,
    "--date_rate", data_rate,
    "--average_session_term", average_session_term,
    "--session_gap", session_gap,
    "--inactive_time", inactive_time,
    "--state_store_path", state_store_path
]