# /bin/sh

ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/je_0418.py conf/memory-event-allowedLate.yml
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/je_0418.py conf/memory-event-allowedLate2500.yml

ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/je_0418.py conf/memory-event-allowedLate5000.yml

sleep 60
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh"
ssh streamix-w "sudo shutdown -h now"
ssh streamix-k "sudo shutdown -h now"
sudo shutdown -h now
