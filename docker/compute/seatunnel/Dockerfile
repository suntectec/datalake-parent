FROM openjdk:11

ARG VERSION
ENV SEATUNNEL_HOME=/opt/seatunnel

RUN apt-get update && apt-get install vim -y && apt-get install tree
RUN echo "alias ll='ls -l'" >> ~/.bashrc

# Download From Internet
# Please Note this file only include fake/console connector, You'll need to download the other connectors manually
RUN cd /opt && \
    wget -P /opt https://dlcdn.apache.org/seatunnel/${VERSION}/apache-seatunnel-${VERSION}-bin.tar.gz && \
    tar -zxvf apache-seatunnel-${VERSION}-bin.tar.gz && \
    mv apache-seatunnel-${VERSION} seatunnel && \
    rm apache-seatunnel-${VERSION}-bin.tar.gz && \
    sed -i 's/#rootLogger.appenderRef.consoleStdout.ref/rootLogger.appenderRef.consoleStdout.ref/' seatunnel/config/log4j2.properties && \
    sed -i 's/#rootLogger.appenderRef.consoleStderr.ref/rootLogger.appenderRef.consoleStderr.ref/' seatunnel/config/log4j2.properties && \
    sed -i 's/rootLogger.appenderRef.file.ref/#rootLogger.appenderRef.file.ref/' seatunnel/config/log4j2.properties && \
    cp seatunnel/config/hazelcast-master.yaml seatunnel/config/hazelcast-worker.yaml

#COPY install-plugin.sh ${SEATUNNEL_HOME}/bin/install-plugin.sh

# plugin_config - this file must created by Unix/Linux Format not Windows for Line Seperator character set diff
# set IDEA Line Seperator Code Style to Unix
COPY plugin_config ${SEATUNNEL_HOME}/config/plugin_config

WORKDIR ${SEATUNNEL_HOME}

#RUN ["chmod", "+x", "bin/install-plugin.sh"]
RUN sh bin/install-plugin.sh ${VERSION}

# SqlServer CDC - Source Driver add to Seatunnel_HOME/lib
RUN wget -P lib https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.5.0.jre8-preview/mssql-jdbc-9.5.0.jre8-preview.jar; \
    wget -P lib https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar; \
    wget -P lib https://repo1.maven.org/maven2/org/apache/seatunnel/seatunnel-transforms-v2/${VERSION}/seatunnel-transforms-v2-${VERSION}.jar

# Transform-V2 - SQL UDF JAR
COPY lib lib