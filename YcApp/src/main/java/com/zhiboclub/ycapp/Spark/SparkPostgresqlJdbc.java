package com.zhiboclub.ycapp.Spark;

import org.apache.spark.sql.SparkSession;

public class SparkPostgresqlJdbc {
    public void query(String sql) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkPostgresqlJdbc")
                .getOrCreate();

        String jdbcUrl = "jdbc:postgresql://gp-bp15a5elb583fb1n8o.gpdb.rds.aliyuncs.com:3432/yicai";
        spark.read().format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("user", "yicai")
                .option("password", "yicai@123")
                .option("url", jdbcUrl)
                .option("query", sql)
                .load()
                .show();

//        spark.stop();

    }

    public static void main(String[] args) {
        new SparkPostgresqlJdbc().query("select count(\"liveId\") total ,\"liveId\" from (select DISTINCT \"userId\",\"liveId\" from events where \"userId\"=53224092 or \"userId\"=713004165 or \"userId\"=799282621   and \"startTime\" < '2019-07-12 09:29:39')a GROUP BY \"liveId\" ORDER BY total desc limit 10");
    }
}
