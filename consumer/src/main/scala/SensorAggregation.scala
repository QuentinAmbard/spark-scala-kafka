/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  * Created by carybourgeois on 10/30/15.
  * Modified by cgilmore on 5/20/16
  */

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneOffset}
import java.util.{Date, UUID}

import com.datastax.demo.fraudprevention.DateUtils
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SensorAggregation extends App {

  // Get configuration properties
  val systemConfig = ConfigFactory.load()
  val appName = systemConfig.getString("TransactionConsumer.sparkAppName")
  val kafkaHost = systemConfig.getString("TransactionConsumer.kafkaHost")
  val kafkaDataTopic = systemConfig.getString("TransactionConsumer.kafkaDataTopic")
  val dseKeyspace = systemConfig.getString("TransactionConsumer.dseKeyspace")
  val dseTable = systemConfig.getString("TransactionConsumer.dseTable")

  // configure the number of cores and RAM to use
  val conf = new SparkConf()
    .setMaster("local[3]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.executor.memory", "1G")
    .setAppName(appName)

  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = SQLContext.getOrCreate(sc)

  case class Sensor(sensor_id: UUID, hive: Int, level: Int, wing: String, latlong: String, sensorType: String)

  case class DailySensor(bucket_ts: Date, sensor_id: UUID, hive: Int, level: Int, wing: String, latlong: String, sensorType: String) {
    def this(bucket_ts: Date, sensor: Sensor) = this(bucket_ts, sensor.sensor_id, sensor.hive, sensor.level, sensor.wing, sensor.latlong, sensor.sensorType)
  }

  case class SensorData (sensorId: UUID, bucketTs: Date, time: Date, value: Float)


  //Could come from a cassandra table with all sensor metadata, or a solr query on distinct sensor ids etc.
  //This is only sensor metadata (1 row per sensor), so if you want 1 hive, or 1 specific level/wing it's very easy to do with a specific schema or solr request.
  val sensorTopologyRDD: RDD[Sensor] = sc.parallelize(Seq(
    Sensor(UUID.fromString("9b952907-750c-11e6-834d-1fcfed080ab0"), 1, 1, "wing1", "123, 4565", "Temperature"),
    Sensor(UUID.fromString("9b952907-750c-11e6-834d-2fcfed080ab0"), 1, 2, "wing2", "22.798227, 119.9727", "Temperature"),
    Sensor(UUID.fromString("9b952907-750c-11e6-834d-5fcfed080ab0"), 3, 77, "North-West-1G", "22.798227, 119.9727", "Temperature")))

  //Let's say we want to compute something for the last 7 days (could be any period of time here)
  //We want as many row in the dataframe as partition. So if we have 7 days and 100 sensors it'll be 700 rows
  val dailySensorTopologyRDD: RDD[DailySensor] = sensorTopologyRDD.flatMap(sensor => {
    val now = LocalDateTime.now(ZoneOffset.UTC)
    for (i <- 0 to 7) yield {
      //bucket_ts is a daily bucket. DateUtils.roundTimestamp takes the midnight value to have the exact bucket time matching any date.
      val bucket_ts = Date.from(DateUtils.roundTimestamp(now.minusDays(i), ChronoUnit.DAYS).atZone(ZoneOffset.UTC).toInstant())
      //Build a DailySensor for each partition key (sensor_id, bucket_ts) in cassandra sensor_data table.
      new DailySensor(bucket_ts, sensor)
    }
  }
  )
  //Shuffle the RDD with the cassandra partitioner for data locality
  val dailySensorTopologyRDDShuffled = dailySensorTopologyRDD.repartitionByCassandraReplica("umbrella", "sensor_data")

  //Now we can perform an efficient join with the data we want. My partition key is (sensor_id, bucket_ts)
  //If you want to do computation on a small period of time (say 1 hour) we could also pushdow the condition on date to cassandra.
  val data: RDD[(DailySensor, SensorData)] = dailySensorTopologyRDDShuffled.joinWithCassandraTable("umbrella", "sensor_data", SomeColumns("sensorId", "bucketTs", "time", "value"))
  //Do any kind of operation on this RDD, let's say we average the Temperature value for each hive
  //You might need to filter on date because we only have a daily resolution here, so if you want to do things at minute or hour level you'll have to filter data in-memory
  val averagePerHive: RDD[(Int, (Int, Float))] = data.map{case (dailySensor, sensorData) => {
    (dailySensor.hive, (1, sensorData.value))
  }}.reduceByKey{case ((count1, sum1), (count2, sum2)) => ((count1 + count2), (sum1 + sum2))}



  val averagePerHive2: RDD[(Int, (Int, Float))] = data.

    .map{case (dailySensor, sensorData) => {
    (dailySensor.hive, (1, sensorData.value))
  }}.reduceByKey{case ((count1, sum1), (count2, sum2)) => ((count1 + count2), (sum1 + sum2))}

  averagePerHive.foreach{case (hiveId, (count, sum)) => println(s"Average for hive $hiveId = ${sum/count.toFloat} ")}

}
