```
docker exec -it seatunnel-client bin/seatunnel.sh -c job/fake2console.sql1-cast.batch.conf
```

```
docker logs -f seatunnel-worker-1 -n 0
```

```
docker logs -f seatunnel-worker-2 -n 0
```