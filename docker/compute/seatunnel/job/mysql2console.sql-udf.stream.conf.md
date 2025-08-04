```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/mysql2console.sql-udf.stream.conf
```

or RESTful API

```
curl --location 'http://192.168.138.15:8080/submit-job/upload' --form 'config_file=@"/opt/poc-allin1/docker/compute/seatunnel/job/mysql2console.sql-udf.stream.conf"'
```

```
docker logs -f seatunnel-worker-1 -n 0
```

```
docker logs -f seatunnel-worker-2 -n 0
```
