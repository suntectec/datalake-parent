# Initialization Dependencies

```
wget -P plugin https://repo1.maven.org/maven2/org/apache/flink/flink-core/1.20.1/flink-core-1.20.1.jar; \
wget -P plugin https://repo1.maven.org/maven2/org/apache/flink/flink-clients/1.20.1/flink-clients-1.20.1.jar; \
wget -P plugin https://repo1.maven.org/maven2/org/apache/flink/flink-streaming-java/1.20.1/flink-streaming-java-1.20.1.jar; \
wget -P plugin https://repo1.maven.org/maven2/org/apache/flink/flink-java/1.20.1/flink-java-1.20.1.jar

```

# Maven Ops

### 在 Maven 构建过程中，特别是配置了 maven-shade-plugin 后，通常会生成两个 JAR 文件：

* **_原始 JAR (Original JAR)_**

名称格式：<artifactId>-<version>.jar（例如：myapp-1.0.jar）

内容：仅包含项目自身编译的类文件（不包含依赖项）。

生成阶段：由 maven-jar-plugin 默认生成，在 package 阶段早期创建。

用途：通常不直接用于运行，除非手动管理所有依赖。

* **_Fat JAR (Shaded JAR)_**

名称格式：<artifactId>-<version>-shaded.jar（或通过插件配置自定义名称）

内容：包含项目自身代码 + 所有依赖库（通过 maven-shade-plugin 打包成一个"fat" JAR）。

生成阶段：在 package 阶段由 maven-shade-plugin 生成，覆盖原始 JAR 或生成新文件（取决于配置）。

用途：可直接运行（如 java -jar shaded.jar），因为所有依赖已内嵌。

# Maven Dependencies Conflicts Resolved - Solution

_mssql-jdbc dependency_: use flink-connector-sqlserver-cdc -> mssql-jdbc:9.4.1.jre8 , remove mssql-jdbc:12.10.1.jre11 

```xml
        <!-- SqlServer CDC Connector -->
        <!-- include mssql-jdbc:9.4.1.jre8 dependency, which url doesn't needs appending Encrypt=false; -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-sqlserver-cdc</artifactId>
            <version>3.4.0</version>
        </dependency>
        <!-- SqlServer 2022 JDBC -->
        <!-- mssql-jdbc:12.10.1.jre11, which url needs appending Encrypt=false;trustServerCertificate=true-->
        <!-- 'connector' = 'sqlserver-cdc', sql create table statement unsupported option encrypt=false; feature disable -->
<!--        <dependency>-->
<!--            <groupId>com.microsoft.sqlserver</groupId>-->
<!--            <artifactId>mssql-jdbc</artifactId>-->
<!--            <version>12.10.1.jre11</version>-->
<!--        </dependency>-->
```