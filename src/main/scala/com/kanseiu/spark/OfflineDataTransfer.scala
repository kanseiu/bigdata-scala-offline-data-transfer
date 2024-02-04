package com.kanseiu.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => F}

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object OfflineDataTransfer {

    /**
     * scp /Users/kanseiu/My-Work/Project/Kanseiu/bigdata-scala-offline-data-transfer/out/artifacts/bigdata_scala_offline_data_transfer_jar/bigdata-scala-offline-data-transfer.jar root@192.168.0.220:/opt; \ssh -l root 192.168.0.220 docker cp /opt/bigdata-scala-offline-data-transfer.jar master:/opt
     *
     * 参数：run_env(运行环境) host(远程IP) table_name(待抽取表名称) partition_name(分区字段) incremental_col_name(增量抽取字段) local_host(本机IP)
     * 本地运行举例：
     * local 192.168.0.220 order_master etl_date modified_time 192.166.22.213
     * local 192.168.0.220 coupon_use etl_date GREATEST(get_time,used_time,pay_time) 192.166.22.213
     * 远程使用spark-submit举例：
     * remote master order_master etl_date modified_time master
     * @param args 运行参数
     */
    def main(args: Array[String]): Unit = {
        if(args.isEmpty || args.length != 6) {
            print("请传入参数...")
            return
        }

        // 定义变量
        val env: String = args(0)
        val host: String = args(1)
        val tableName: String = args(2)
        val partitionName: String = args(3)
        val incrementalColName: String = args(4)
        val localHost: String = args(5)
        val hiveDb: String = "ods"
        val hiveTable: String = hiveDb + "." + tableName
        val sparkWarehouse: String = s"hdfs://$host:9000/user/hive/warehouse"
        val metastoreUris: String = s"thrift://$host:9083"
        var mysqlPort: String = "3306"

        println("当前运行环境: " + env)

        // 创建 sparkSessionBuilder
        val sparkSessionBuilder: SparkSession.Builder = SparkSession.builder
          .appName(tableName + " incremental data transfer")
          .config("spark.sql.warehouse.dir", sparkWarehouse)
          .config("hive.metastore.uris", metastoreUris)
          .config("spark.executor.memory", "512m") // 根据需要设置 executor 内存
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()

        if("local".equals(env)) {
            mysqlPort = "33060"
            sparkSessionBuilder
              .master(s"spark://$host:7077") // 指定远程 Spark 集群的 master 节点
              .config("spark.driver.host", localHost) // 指定本机 IP 地址
        }

        // 创建 sparkSession
        val sparkSession: SparkSession = sparkSessionBuilder.getOrCreate()

        // 设置etl_date
        val today: LocalDate = LocalDate.now()
        val yesterday: LocalDate = today.minusDays(1)
        val partitionValue: String = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

        println("partitionValue: " + partitionValue)

        // 定义基本的jdbc连接选项
        val jdbcOptions: Map[String, String] = Map(
            "driver" -> "com.mysql.jdbc.Driver",
            "url" -> s"jdbc:mysql://$host:$mysqlPort/ds_db01?useSSL=false&characterEncoding=UTF-8",
            "user" -> "root",
            "password" -> "123456"
        )

        // 获取ods对应表的、作为增量抽取条件的最大值（最大时间）
        val queryOdsIncrementalColMax: DataFrame = sparkSession.sql(s"select max($incrementalColName) from $hiveTable")

        val resultFirst = queryOdsIncrementalColMax.head(1)

        // 这里做了一些处理，当ods对应的表没有数据的时候，从mysql全量抽取数据，否则按照字段增量抽取
        // 检查查询结果是否为空
        if (resultFirst.isEmpty || resultFirst(0).isNullAt(0)) {
            println("全量抽取表" + tableName)
            // 如果查询结果为空，则全量抽取数据
            val fullLoadDF: DataFrame = sparkSession.read
              .format("jdbc")
              .options(jdbcOptions)
              .option("dbtable", tableName)
              .load()
              .withColumn(partitionName, F.lit(partitionValue))

            fullLoadDF.write
              .format("hive")
              .mode(SaveMode.Overwrite)
              .partitionBy(partitionName)
              .saveAsTable(hiveTable)
        } else {
            println("增量抽取表" + tableName)
            // 如果查询结果不为空，则增量抽取数据
            val odsIncrementalColMax: Timestamp = queryOdsIncrementalColMax.first.getAs[java.sql.Timestamp](0)
            // 增量查询SQL
            val querySql: String = s"SELECT * FROM $tableName WHERE $incrementalColName > '$odsIncrementalColMax'"

            val incrementalLoadDF: DataFrame = sparkSession.read
              .format("jdbc")
              .options(jdbcOptions)
              .option("query", querySql)
              .load()
              .withColumn(partitionName, F.lit(partitionValue))

            incrementalLoadDF.write
              .format("hive")
              .mode(SaveMode.Append)
              .partitionBy(partitionName)
              .saveAsTable(hiveTable)
        }

        // 查看分区情况
        sparkSession.sql(s"SHOW PARTITIONS $hiveTable").show()

        // 查看行数
        sparkSession.sql(s"SELECT COUNT(1) from $hiveTable").show()

        sparkSession.stop()
    }
}