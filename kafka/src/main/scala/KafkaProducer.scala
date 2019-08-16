package com.eztier.examples

import java.util.Properties
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

def buildKafkaProducerForHl7 () : KafkaProducer[String, String] = {
  val kafkaBrokers = "127.0.0.1:9092,172.17.0.1:9010,172.17.0.1:9096"
  val props = new Properties()
  props.put("bootstrap.servers", kafkaBrokers)
  props.put("acks", "all")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  return producer
}

val kafka = buildKafkaProducerForHl7()
val hl7Topic ="ADT"
