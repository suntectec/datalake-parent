### Submit Job

```
docker exec -it seatunnel-client bin/seatunnel.sh \
-c job/sqlserver2sqlserver.cdc.stream.conf
```

### Console

```
docker logs -f seatunnel-worker-1 -n 0
```

```
docker logs -f seatunnel-worker-2 -n 0
```