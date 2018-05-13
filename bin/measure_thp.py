import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("broker_address")
parser.add_argument("measuring_time", type=int)

args = parser.parse_args()
thp_list = []
for i in range(0, 6):
    command_line = [
        "java", "-cp",
        "./target/gw-stream-bench-1.0-SNAPSHOT-shaded.jar",
        "edu.snu.splab.gwstreambench.sink.KafkaThpCounter",
        "-b", args.broker_address,
        "-m", str(args.measuring_time),
    ]
    subprocess.call(command_line)
    if i != 0:
        output = open("./thp.txt", "r")
        thp = float(output.readline().strip())
        thp_list.append(thp)

sum = 0.
for thp in thp_list:
    sum += thp

print "Throughput = %f" % (sum / len(thp_list))
