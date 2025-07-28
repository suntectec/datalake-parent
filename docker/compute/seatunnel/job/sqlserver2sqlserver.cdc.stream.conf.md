### Submit Job

```
docker exec -it seatunnel-client bin/seatunnel.sh \
-c job/sqlserver2sqlserver.cdc.stream.conf
```

or RESTful API

```
curl --location 'http://192.168.138.15:8080/submit-job/upload' --form 'config_file=@"/opt/poc-allin1/docker/compute/seatunnel/job/sqlserver2sqlserver.cdc.stream.conf"'
```

### Console

```
docker logs -f seatunnel-worker-1 -n 0
```

```
docker logs -f seatunnel-worker-2 -n 0
```