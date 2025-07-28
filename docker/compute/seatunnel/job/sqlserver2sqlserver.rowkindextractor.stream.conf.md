```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/sqlserver2sqlserver.rowkindextractor.stream.conf
```

or RESTful API

```
curl --location 'http://192.168.138.15:8080/submit-job/upload' --form 'config_file=@"/opt/poc-allin1/docker/compute/seatunnel/job/sqlserver2sqlserver.rowkindextractor.stream.conf"'
```