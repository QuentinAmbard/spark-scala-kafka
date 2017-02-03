package com.datastax.demo.fraudprevention

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

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.{Date, TimeZone, UUID}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import net.liftweb.json._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


object TransactionConsumer extends App {

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
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint(appName)

  // configure kafka connection and topic
  val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaHost)
  val kafkaTopics = Set(kafkaDataTopic)
  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

  case class SensorData(serial_number: String, sensor_snapshot: Date, floor: Int, hive_num: Int, humidity: Float, latitude: Float, longitude: Float,
                        main_history: List[String], manuf_date: Date, movement: Int, retired: Boolean, sensor_type: String, temperature: Float, vendor: String, wing: String)

  case class sensorDataC (sensorId: UUID, bucketTs: Date, time: Date, hive: Int, level: Int, wing: String, sensorType: String, value: Float, latlong: String)

  implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  }
  kafkaStream.window(Seconds(1), Seconds(1))
    .foreachRDD { (message: RDD[(String, String)], batchTime: Time) => {
      message.map { case (k, v) => {
        val sensorData = parse(v).extract[SensorData]
        val latLong = sensorData.latitude+", "+sensorData.longitude
        val value = sensorData.sensor_type match {
          case "Temperature" => sensorData.temperature
          case "Humidity" => sensorData.humidity
          case "Movement" => sensorData.movement
        }
        val ldt = LocalDateTime.ofInstant(sensorData.sensor_snapshot.toInstant(), ZoneOffset.UTC)
        val bucket_ts = DateUtils.roundTimestamp(ldt, ChronoUnit.DAYS)
        val bucket_ts_date = Date.from(bucket_ts.atZone(ZoneOffset.UTC).toInstant())
        sensorDataC(UUID.fromString(sensorData.serial_number), bucket_ts_date, sensorData.sensor_snapshot, sensorData.hive_num, sensorData.floor, sensorData.wing, sensorData.sensor_type, value, latLong)
      }
      }.saveToCassandra("umbrella", "sensor_data")
    }
    }
  ssc.start()
  ssc.awaitTermination()
}
