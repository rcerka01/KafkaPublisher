package itv.dispatch.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.util.Properties
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}

import java.time.Duration
import java.util.concurrent.Future

class Kafka(bootstrapServers: String) {
  // producer
  private lazy val producer: KafkaProducer[String, String] = {
    val producerProperties = new Properties()
    producerProperties.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      bootstrapServers
    )
    producerProperties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )
    producerProperties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )
    new KafkaProducer[String, String](producerProperties)
  }

  def getProducer(): KafkaProducer[String, String] = producer

  def closeProducer(): Unit = {
    producer.close()
  }
}
