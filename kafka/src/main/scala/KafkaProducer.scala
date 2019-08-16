package com.eztier.examples

import java.util.Properties
import org.apache.kafka.clients.producer.{Producer, KafkaProducer, ProducerRecord}

object KafkaProducer {
  private val kafkaBrokers = "127.0.0.1:9092,172.17.0.1:9010,172.17.0.1:9096"
    
  private def buildKafkaProducerForHl7 () : KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBrokers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    return producer
  }

  private val producer = buildKafkaProducerForHl7()
  private val hl7Topic ="ADT"
  private val r = new scala.util.Random()
    
  private val padLeftZero: Int => String = in: Int => if (in < 10) "0" + in.toString else in.toString
  private val getRandomWithRange: (Int, Int) => Int = (min: Int, max: Int) => max + r.nextInt(( max - min) + 1)
  
  private val getRandomMickeyAdt: () => (String, String) = () => {
    val randomMrn = (for (i <- 1 to 7) yield r.nextInt(9)).map(_.toString).mkString("")
    val randomDOB = getRandomWithRange(1928, 2019).toString + padLeftZero(getRandomWithRange(1, 12)) + padLeftZero(getRandomWithRange(1, 28)) 
    
    val msg = List(
      "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20110613072049||ADT^A08|934579920110613072049|P|2.3||||",
      s"PID|1||${randomMrn}||MOUSE^MICKEY^||${randomDOB}|M||W|123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^^^theMainMouse@disney.com|||||1719|99999999|||N~U|||||||||||||||||"
    ).mkString("\r")
    
    (randomMrn, msg)
  }
  
  val produce = { 
  
    while (true) {
      val nextMickey = getRandomMickeyAdt()
      
      val data = new ProducerRecord[String, String](hl7Topic, nextMickey._1, nextMickey._2)
      
      producer.send(data)
    
      Thread.sleep(1000)
    }
  
  }
}
