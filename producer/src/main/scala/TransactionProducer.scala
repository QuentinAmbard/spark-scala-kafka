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
  * Created by carybourgeois on 3/17/16.
  */
package com.datastax.demo.fraudprevention

import java.sql.Timestamp
import java.util.Properties
import java.util.UUID._
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source

class produceTransactions(brokers: String, topic: String) extends Actor {

  val merchList = new Merchant
  val locList = new Location

  val r = scala.util.Random

  val numProviders = 9999
  val numCards = 99999999
  val numUniqueItems = 99999
  val maxNumItemsminusOne = 4  // The Minus one is here because we will add one to the number to correct for the possibility of zero


  private val stream = getClass.getResourceAsStream("/single-sensor-10000.json")
  val lines = Source.fromInputStream(stream).getLines().toList
  val cursor = new AtomicInteger(0)
  object kafka {
    val producer = {
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.StringEncoder")

      val config = new ProducerConfig(props)
      new Producer[String, String](config)
    }
  }

  def receive = {
    case numTrans : Int => {
      val messages = for (sensor <- 1 to numTrans ) yield {
        val lineNumber = cursor.getAndIncrement()
        if(lineNumber > lines.size - 2) cursor.set(0)
        val str = lines(lineNumber)
        new KeyedMessage[String, String](topic, str)
      }
      kafka.producer.send(messages: _*)
    }
    case _ => println("Not a valid message!")
  }

  def createItems (numItems: Int) : (String, String) = {
    var total = 0.0
    val message = for (item <- 1 to numItems) yield {
      val itemVal = r.nextDouble()*1000
      total = total + itemVal
      s"Item_${r.nextInt(numUniqueItems)}->" + "%1.2f".format(itemVal)
    }
    return (s"${message.mkString(",")}", "%1.2f".format(total))
  }
}


object TransactionProducer extends App {
  /*
   * Get runtime properties from application.conf
   */
  val systemConfig = ConfigFactory.load()

  /*
   * Kafka Properties
   */
  val kafkaHost = systemConfig.getString("TransactionProducer.kafkaHost")
  println(s"kafkaHost $kafkaHost")
  val kafkaTopic = systemConfig.getString("TransactionProducer.kafkaTopic")
  println(s"kafkaTopic $kafkaTopic")

  /*
   * Application Properties
   */
  val maxNumTransPerWait = systemConfig.getInt("TransactionProducer.maxNumTransPerWait")
  println(s"maxNumTransPerWait $maxNumTransPerWait")
  val waitMillis = systemConfig.getLong("TransactionProducer.waitMillis")
  println(s"waitMillis $waitMillis")
  val runDurationSeconds = systemConfig.getLong("TransactionProducer.runDurationSeconds")
  println(s"runDurationSeconds $runDurationSeconds")

  /*
   * Set up the Akka Actor
   */
  val system = ActorSystem("TransactionProducer")
  val messageActor = system.actorOf(Props(new produceTransactions(kafkaHost, kafkaTopic)), name="genTransactions")

  /*
   * Message Loop
   */
  val r = scala.util.Random
  var numTransCreated : Long = 0
  val stopTime = System.currentTimeMillis() + (runDurationSeconds * 1000)
  while(runDurationSeconds < 0 || System.currentTimeMillis() < stopTime) {
    val numTrans = r.nextInt(maxNumTransPerWait) + 1
    messageActor ! numTrans
    numTransCreated += numTrans
    println(s"${numTransCreated} Transactions created.")
    Thread sleep waitMillis
  }

}
