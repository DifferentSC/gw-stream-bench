# /bin/sh
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
sleep 5
../flink-nvme/build-target/bin/stop-cluster.sh && ../flink-nvme/build-target/bin/start-cluster.sh
sleep 5
python bin/large_scale_window.py conf/large-scale-window10000.yml
#python bin/general_window_samza_vldb.py conf/.yml

