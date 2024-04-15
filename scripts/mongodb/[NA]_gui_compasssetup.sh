#!/bin/bash

# Download MongoDB Compass for Linux

echo "Downloading MongoDB Compass..."
curl -O https://downloads.mongodb.com/compass/mongodb-compass_1.42.5_amd64.deb

# Install MongoDB Compass
echo "Installing MongoDB Compass..."
sudo dpkg -i mongodb-compass_1.42.5_amd64.deb

# Check for any missing dependencies
echo "Checking for missing dependencies..."
sudo apt install -f

# Clean up downloaded package
echo "Cleaning up..."
rm mongodb-compass_1.42.5_amd64.deb

# Launch MongoDB Compass
echo "Launching MongoDB Compass..."
mongodb-compass &
