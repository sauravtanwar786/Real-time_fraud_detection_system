# Realtime Credit Card Fraud Detection System
This repository implements a real-time credit card fraud detection pipeline using Debezium, Kafka, Spark and Cassandra. A realtime data replication service using Debezium and Kafka replicates transaction data and processing via Spark Stremaing job. Meanwhile, classified transaction records will be displayed on the dashboard for visualization.

## Project Design

### Prerequisites
1.[Docker version >= 20.10.17](https://docs.docker.com/engine/install/) Make sure that docker is running using 'docker ps'

**Windows users**:  please setup WSL and a local Ubuntu Virtual machine following **[the instructions here](https://ubuntu.com/tutorials/install-ubuntu-on-wsl2-on-windows-10#1-overview)**. Install the above prerequisites on your ubuntu terminal; if you have trouble installing docker, follow **[the steps here](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-22-04#step-1-installing-docker)** (only Step 1 is necessary). Please install the make command with `sudo apt install make -y` (if its not already present).

### Setup

All the commands shown below are to be run via the terminal (use the Ubuntu terminal for WSL users). We will use docker to set up our containers. Clone and move into the lab repository, as shown below.

We have some helpful make commands to make working with our systems more accessible. Shown below are the make commands and their definitions

### Architecture


make up
make connectors
http://localhost:9047/signup -> make account user name dremio password dremio123
./dremio-init.sh
http://localhost:8088/sqllab/ 