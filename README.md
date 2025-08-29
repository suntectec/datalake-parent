# Trying out Apache Paimon, Flink and SeanTunnel for DataLake - Proof of Concept

![Static Badge](https://img.shields.io/badge/Apache-Paimon-blue?logo=apache&logoColor=%23E6526F&labelColor=black)
![Static Badge](https://img.shields.io/badge/Apache-Flink-blue?logo=apache&logoColor=%23E6526F&labelColor=black)
![Static Badge](https://img.shields.io/badge/Apache-SeaTunnel-blue?logo=apache&logoColor=%23E6526F&labelColor=black)

## Modules Here:

1. [x] **_docker_**
   Running DataLake Docker Containers using Dockerfile and Docker Compose is a convenient way to get up and running to try
   out some DataLake workloads.
   Assuming docker is installed you can start the containers using the following.
   Operation Linux host location: `/opt/poc-allin1`
   * **_compute_**
   Flink Client SQL Scripts Jobs Compute System
   SeaTunnel Config Jobs Compute System
   * **_data_**
   Database Storage System
1. [x] **_flink_**
Flink Java Coding Program with DataStream API & Table API & SQL Example Jobs
1. [x] **_native_**
Local Download Flink and SeaTunnel for AB Control Groups Experiments
1. [x] **_seatunnel_**
SeaTunnel UDF Examples

### For entering workdir-path faster, using setting alias in bashrc:

```shell
echo "alias pp='cd /opt/poc-allin1'" >> ~/.bashrc

source ~/.bashrc
```