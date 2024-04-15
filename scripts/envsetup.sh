#!/bin/bash

echo "Updating package lists..."
sudo apt update

echo "Installing required packages..."
sudo apt install -y python3 python3-pip default-jre default-jdk vim

echo "Displaying Java and Java compiler versions..."
java --version
javac --version

echo "Installing MongoDB package..."
bash ~/src/scripts/mongodb/install.sh

echo "Starting MongoDB service..."
systemctl start mongod

echo "Checking MongoDB service status..."
systemctl status mongod

echo "Configuring MongoDB to allow connections from external IPs..."
sudo sed -i 's/^bind_ip = 127.0.0.1/bind_ip = 0.0.0.0/' /etc/mongodb.conf
# echo "Configuring MongoDB to allow connections from specific IPs..."
# sudo sed -i '/^# network interfaces/a net:\n  bindIp: 127.0.0.1,<ip_address_1>,<ip_address_2>' /etc/mongod.conf

echo "Allowing MongoDB port through firewall..."
sudo ufw allow 27017

echo "Restarting MongoDB service..."
sudo systemctl restart mongod

echo "Installing Python packages from requirements.txt..."
pip3 install -r requirements.txt

echo "Downloading and extracting Spark binaries..."
curl -O https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar xvf spark-3.5.1-bin-hadoop3.tgz 
mv spark-3.5.1-bin-hadoop3 /opt/spark

echo "Downloading and extracting Kafka binaries..."
curl -O https://downloads.apache.org/kafka/3.7.0/kafka_2.12-3.7.0.tgz
tar xvf kafka_2.12-3.7.0.tgz
mv kafka_2.12-3.7.0 /opt/kafka

echo "Configuring environment variables..."
cat <<EOF >> ~/.bashrc
# Set Spark path and add to PATH
export SPARK_PATH=/opt/spark
export PATH=\$PATH:\$SPARK_PATH/sbin

# Set Kafka home and add to PATH
export KAFKA_HOME=/opt/kafka
export PATH=\$PATH:\$KAFKA_HOME/bin

# Set MongoDB path and add to PATH
export MONGODB_HOME=/bin/mongosb
export PATH=\$PATH:\$MONGODB_HOME
EOF

echo "Reloading bashrc..."
source ~/.bashrc

echo "Installation completed successfully."
