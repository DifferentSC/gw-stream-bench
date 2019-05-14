import argparse
import subprocess
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument("exp_mode")
parser.add_argument("text_file_path")
parser.add_argument("tuple_num", type=int)
parser.add_argument("key_num", type=int)
parser.add_argument("skewness", type=float)
parser.add_argument("margin", type=int)
args = parser.parse_args()

# Create the datafile
make_file_command_line = [
    "java", "-cp",
    "./source-sink/target/source-sink-1.0-SNAPSHOT-shaded.jar",
    "edu.snu.splab.gwstreambench.source.FileSamzaExpDataGen",
    "-f", args.text_file_path,
    "-n", str(args.tuple_num),
    "-k", str(args.key_num),
    "-s", str(args.skewness),
    "-m", str(args.margin)
]

if not os.path.isfile(args.text_file_path):
    subprocess.call(make_file_command_line)
    print "Finished creating data file..."
else:
    print "Use already existing file..."

flink_command_line = None
flink_common_command_line = [
    "flink", "run",
    "./samza-vldb-simul/target/samza-vldb-simul-1.0-SNAPSHOT-shaded.jar",
    "--broker_address", "localhost:9092",
    "--zookeeper_address", "localhost:2181",
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
        "--block_cache_size", str(40960),
        "--write_buffer_size", str(128)
    ]

elif args.exp_mode == "streamix_sata":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "streamix",
        "--state_store_path", "/tmp"
    ]

elif args.exp_mode == "streamix_nvme":
    flink_command_line = flink_common_command_line + [
        "--state_backend", "streamix",
        "--state_store_path", "/nvme",
        "--batch_write_size", str(10000),
        "--file_num", str(1)
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
    with open("job_list.txt", "r") as job_list_file:
        lines = job_list_file.readlines()
        if lines[1].strip().startswith("No running jobs"):
            break

elapsed_time = time.time() - start_time
thp = args.tuple_num / elapsed_time
print "Thp = %f" % thp
