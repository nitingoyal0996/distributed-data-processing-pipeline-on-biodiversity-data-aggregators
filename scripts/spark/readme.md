# Running Spark Cluster #

## Pre-requisites ##
1. Install dependencies

- run the `../install.sh` file for system setup
- run the `pip install requirements.txt` for python packages

## Running spark ## 
On the master node 

```bash
bash src/scripts/spark/start-master.sh
```

On the worker nodes

```bash
bash src/scripts/spark/start-worker.sh
```


## Monitoring ## 
Spark cluster is accessible via port `8080` of the master node.

```bash
# get the hostname
hostname -a

# access the portal via - hostname:8080
```

## Submitting a new job ##

```bash
bash src/scripts/spark/submit-job.sh
```

It's going to submit the job to the master, which then distribute the work among the worker nodes.