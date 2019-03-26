# /bin/sh

:<<'END'
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/rocksdb-session-window-100K.yml

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/rocksdb-session-window-10K.yml

ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.1-100K.yml

sleep 60
END
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.2-100K.yml

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.3-100K.yml

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.1-10K.yml

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.2-10K.yml
END

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.3-10K.yml

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.1-1K.yml

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.2-1K.yml
END

sleep 60
ssh streamix-w 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"'
sleep 5
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh && /home/ubuntu/flink-nvme/build-target/bin/start-cluster.sh"
sleep 5
python bin/general_window_samza_vldb.py conf/streamix-session-window-0.3-1K.yml

sleep 60
ssh streamix-k "/home/ubuntu/flink-nvme/build-target/bin/stop-cluster.sh"
ssh streamix-w "sudo shutdown -h now"
ssh streamix-k "sudo shutdown -h now"
sudo shutdown -h now
