import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("broker_address")
parser.add_argument("event_rate", type=int)
parser.add_argument("timer_num", type=int)
parser.add_argument("key_num", type=int)
parser.add_argument("skewness", type=float)

args = parser.parse_args()

command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.KafkaWordGeneratingSource",
    "-b", args.broker_address,
    "-r", str(args.event_rate),
    "-t", str(args.timer_num),
    "-k", str(args.key_num),
    "-s", str(args.skewness)
]

subprocess.call(command_line)
