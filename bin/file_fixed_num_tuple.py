import argparse
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("exp_mode")
parser.add_argument("text_file_path")
parser.add_argument("tuple_num", type=int)
parser.add_argument("key_num", type=int)
parser.add_argument("skewness", type=float)
parser.add_argument("window_size", type=int)
parser.add_argument("sliding_interval", type=int)

args = parser.parse_args()

# Create the datafile
fill_window_command_line = [
    "java", "-cp",
    "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.FileFixNumTupleGenSource",
    "-f", args.text_file_path,
    "-n", str(args.window_size),
    "-k", str(args.key_num),
    "-s", str(args.skewness)
]

subprocess.call(fill_window_command_line)

print "Finished creating data file..."

flink_command_line = None
flink_common_command_line = [
    "flink", "run",
    "/home/gyewon/gw-stream-bench/target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", "localhost:9092",
    "--zookeeper_address", "localhost:2181",
    "--window_size", str(args.window_size),
    "--sliding_interval", str(args.sliding_interval),
    "--text_file_path", str(args.text_file_path)
]

state_backend_command_line = []

if args.exp_mode == "mem":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "mem"
    ]

elif args.exp_mode == "rocksdb_sata":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "rocksdb",
        "--rocksdb_path", "/tmp",
        "--block_cache_size", str(0)
    ]

elif args.exp_mode == "rocksdb_nvme":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "rocksdb",
        "--rocksdb_path", "/nvme",
        "--block_cache_size", str(0)
    ]

elif args.exp_mode == "file_sata":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "file",
        "--state_store_path", "/home/gyewon/state_tmp"
    ]

elif args.exp_mode == "file_nvme":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "file",
        "--state_store_path", "/nvme"
    ]

print flink_command_line
subprocess.Popen(flink_command_line)
start_time = time.time()
time.sleep(1)
while True:
    with open("job_list.txt", "w") as job_list_file:
        subprocess.call([
            "flink", "list"
        ], stdout=job_list_file)
    with open("job_list.txt", "w") as job_list_file:
        lines = job_list_file.readlines()
        if lines[1].strip().startswith("No running jobs"):
            break

elapsed_time = time.timed() - start_time
thp = args.tuple_num / elapsed_time
print "Thp = %f" % thp
