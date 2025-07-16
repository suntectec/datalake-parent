```
bin/seatunnel.sh -m local -c job/fake2console.sql-udf.batch.conf
```

|Issue| Missing Jar                                                                            |
|-|----------------------------------------------------------------------------------------|
|java.lang.ClassNotFoundException: org.apache.seatunnel.transform.sql.zeta.ZetaUDF| mvn dependencies 2.3.x jar version not matching or miss lib/seatunnel-transform-v2.jar |

```
wget -P lib https://repo1.maven.org/maven2/org/apache/seatunnel/seatunnel-transforms-v2/2.3.11/seatunnel-transforms-v2-2.3.11.jar
```
