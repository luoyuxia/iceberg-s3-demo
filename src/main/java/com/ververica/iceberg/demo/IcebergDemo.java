package com.ververica.iceberg.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class IcebergDemo {

    // add the following ak/sk to flink-conf
//    flink.hadoop.fs.s3a.endpoint: xxx
//    flink.hadoop.fs.s3a.access.key: xxx
//    flink.hadoop.fs.s3a.secret.key: xxx

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("create table data_gen(id int, data string) with ('connector' = 'datagen')");

        tableEnv.executeSql("create table iceberg_table (id int, data string)\n" +
                "with ('connector' = 'iceberg', 'catalog-name' = 'test_iceberg_catalog', 'catalog-name' = 'hadoop_prod',\n" +
                "'catalog-type'='hadoop',\n" +
                "'warehouse' = 's3a://bucket_name/iceberg-data'\n" +
                ")\n");
        tableEnv.executeSql("INSERT INTO iceberg_table select * from data_gen").await();
    }
}
