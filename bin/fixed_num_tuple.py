import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("broker_address")
parser.add_argument("tuple_num", type=int)
parser.add_argument("key_num", type=int)
parser.add_argument("skewness", type=float)
parser.add_argument("window_size", type=int)

args = parser.parse_args()

# Fill the window firstly...
fill_window_command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "KafkaFixNumTupleGenSource",
    "-b", args.broker_address,
    "-n", str(args.window_size),
    "-k", str(args.key_num),
    "-s", str(args.skewness)
]

subprocess.Popen(fill_window_command_line)

# Ensure that all the previous events are processed...
wait_command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "KafkaFixedNumTupleThpCounter",
    "-b", args.broker_address,
    "-n", str(args.window_size)
]
subprocess.call(wait_command_line)

source_command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "KafkaFixNumTupleGenSource",
    "-b", args.broker_address,
    "-n", str(args.tuple_num),
    "-k", str(args.key_num),
    "-s", str(args.skewness)
]

# Start source in an asynchronous way
subprocess.Popen(source_command_line)

log_command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "KafkaFixedNumTupleThpCounter",
    "-b", args.broker_address,
    "-n", str(args.tuple_num),
]

# Start logging
subprocess.call(log_command_line)
