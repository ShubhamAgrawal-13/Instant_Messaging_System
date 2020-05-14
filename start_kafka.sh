gnome-terminal --tab --title="zookeeper" -- bash -c "/home/shubham/Downloads/kafka_2.12-2.4.0/bin/zookeeper-server-start.sh /home/shubham/Downloads/kafka_2.12-2.4.0/config/zookeeper.properties; exec bash"
sleep 10
gnome-terminal --tab --title="Kafka" -- bash -c "/home/shubham/Downloads/kafka_2.12-2.4.0/bin/kafka-server-start.sh /home/shubham/Downloads/kafka_2.12-2.4.0/config/server.properties; exec bash"
