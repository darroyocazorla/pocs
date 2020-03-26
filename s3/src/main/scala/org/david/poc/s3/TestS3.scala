/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.david.poc.s3

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestS3 extends App {

  val accessKey =  "XXX"
  val secretKey = "XXX"

  val s3endopoint = "s3.eu-west-3.amazonaws.com"
  val bucket = "XXX"

  val conf =
    new SparkConf()
      .setAppName("test")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.hadoop.fs.s3a.access.key", accessKey)
      .set("spark.hadoop.fs.s3a.secret.key", secretKey)
      .set("spark.hadoop.fs.s3a.endpoint", s3endopoint)
      .set("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")

  System.setProperty("com.amazonaws.services.s3.enableV4", "true")

  val filePath = s"s3a://$bucket/tmp/test.parquet"

  val sparkSession =  SparkSession.builder().config(conf).getOrCreate()

  sparkSession.sql("SELECT 1 as id").write.parquet(filePath)
  sparkSession.read.parquet(filePath).show()

}
