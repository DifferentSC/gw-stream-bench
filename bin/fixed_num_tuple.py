import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("broker_address")
parser.add_argument("tuple_num", type=int)
parser.add_argument("key_num", type=int)
parser.add_argument("skewness", type=float)

args = parser.parse_args()

source_command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.KafkaFixNumTupleGenSource",
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
    "edu.snu.splab.gwstreambench.sink.KafkaFixedNumTupleThpCounter",
    "-b", args.broker_address,
    "-n", str(args.tuple_num),
]

# Start logging in a synchronous way
subprocess.call(log_command_line)
