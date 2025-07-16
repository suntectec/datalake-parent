# Dockerfile Build

PS Attention: seatunnel/plugin_config - this file must firstly created by Linux not Windows for character set diff
or set IDEA Line Seperator Code Style to Unix

[阿里巴巴Java开发手册，强制要求IDE 的 text file encoding 设置为 UTF-8; IDE 中文件的换行符使用 Unix 格式，不要使用 Windows 格式。](https://blog.csdn.net/m0_63628018/article/details/149117369?spm=1001.2014.3001.5502)

[阿里巴巴开发手册-代码格式-第九条](https://alibaba.github.io/p3c/%E7%BC%96%E7%A8%8B%E8%A7%84%E7%BA%A6/%E4%BB%A3%E7%A0%81%E6%A0%BC%E5%BC%8F.html)

```
docker build -f Dockerfile --build-arg VERSION=2.3.11 -t seatunnel:2.3.11-jdk-11 .
```

# Address already in use

**Issue**: Error response from daemon: failed to set up container networking: Address already in use

**Resolved**: `sudo lsof -i tcp:端口号` or `netstat -tulnp|grep 端口号`, then `kill -9 [pid number]`

```
sudo lsof -i tcp:18080
sudo lsof -i tcp:15801

netstat -tulnp|grep 8080
netstat -tulnp|grep 5801
```

**PS**: change service client of ipv4_address to idle address

