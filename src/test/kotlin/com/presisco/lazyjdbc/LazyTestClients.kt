package com.presisco.lazyjdbc

abstract class LazyOracleTestClient : LazyJdbcClientTest(
        "dataSourceClassName" to "oracle.jdbc.pool.OracleDataSource",
        "dataSource.url" to "jdbc:oracle:thin:@//192.168.1.201:1521/XE",
        "dataSource.user" to "SAMPLE",
        "dataSource.password" to "sample",
        "maximumPoolSize" to "5"
)

abstract class LazyMySQLTestClient : LazyJdbcClientTest(
        "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
        "dataSource.url" to "jdbc:mysql://192.168.1.202:3306/sample?useUnicode=true&characterEncoding=utf-8",
        "dataSource.user" to "root",
        "dataSource.password" to "root",
        "maximumPoolSize" to "5"
)