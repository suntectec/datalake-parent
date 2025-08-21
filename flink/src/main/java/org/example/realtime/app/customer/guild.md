sqlserver-transaction-log-source

SqlServer CDC Debezium:

```shell
#初始化
Struct{after=Struct{id=2,order_id=6eaa804c-5d1d-4b2f-ac92-021783a10d87,supplier_id=3016,item_id=47,status=shipped,qty=600,net_price=1310,issued_at=1753243341153,completed_at=1753243341153,created_at=1753243341153,updated_at=1753271606100},source=Struct{version=1.9.8.Final,connector=sqlserver,name=sqlserver_transaction_log_source,ts_ms=1755583078987,snapshot=last,db=inventory,schema=INV,table=orders,commit_lsn=0000002e:00001d78:0003},op=r,ts_ms=1755583079060}

#新增
Struct{after=Struct{id=4,order_id=111,supplier_id=3016,item_id=47,status=shipped,qty=600,net_price=1310,issued_at=1753243341153,completed_at=1753243341153,created_at=1753243341153,updated_at=1753271606100},source=Struct{version=1.9.8.Final,connector=sqlserver,name=sqlserver_transaction_log_source,ts_ms=1755583161890,db=inventory,schema=INV,table=orders,change_lsn=0000002e:00001f78:0002,commit_lsn=0000002e:00001f78:0008,event_serial_no=1},op=c,ts_ms=1755583165514}

#更新
Struct{before=Struct{id=4,order_id=111,supplier_id=3016,item_id=47,status=shipped,qty=600,net_price=1310,issued_at=1753243341153,completed_at=1753243341153,created_at=1753243341153,updated_at=1753271606100},after=Struct{id=4,order_id=111,supplier_id=3016,item_id=47,status=shipped,qty=800,net_price=1310,issued_at=1753243341153,completed_at=1753243341153,created_at=1753243341153,updated_at=1753271606100},source=Struct{version=1.9.8.Final,connector=sqlserver,name=sqlserver_transaction_log_source,ts_ms=1755583230730,db=inventory,schema=INV,table=orders,change_lsn=0000002e:00002028:0002,commit_lsn=0000002e:00002028:0003,event_serial_no=2},op=u,ts_ms=1755583236746}

#删除
Struct{before=Struct{id=4,order_id=111,supplier_id=3016,item_id=47,status=shipped,qty=800,net_price=1310,issued_at=1753243341153,completed_at=1753243341153,created_at=1753243341153,updated_at=1753271606100},source=Struct{version=1.9.8.Final,connector=sqlserver,name=sqlserver_transaction_log_source,ts_ms=1755583267957,db=inventory,schema=INV,table=orders,change_lsn=0000002e:00002070:000d,commit_lsn=0000002e:00002070:000f,event_serial_no=1},op=d,ts_ms=1755583269931}
```