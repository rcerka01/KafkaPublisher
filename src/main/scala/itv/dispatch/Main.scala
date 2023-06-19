package itv.dispatch

import itv.dispatch.kafka.Kafka
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.StdIn
import scala.util.Random

enum Command:
  case Forward, Clockwise, Anticlockwise

object Main {
  private val TOPIC_NAME = "enum-topic"
  private val BOOTSTRAP_SERVERS = "127.0.0.1:9092"
  private val kafka = new Kafka(BOOTSTRAP_SERVERS)
  private lazy val kafkaProducer = kafka.getProducer()

  def main(args: Array[String]): Unit = {
    try {
      while (true) {
        val command = Command.values(Random.nextInt(Command.values.length))
        sendCommand(kafkaProducer, command, TOPIC_NAME)
        Thread.sleep(3000)
      }
    } finally {
      kafka.closeProducer()
    }
  }

  private def sendCommand(
                   producer: KafkaProducer[String, String],
                   command: Command,
                   topicName: String
                 ): Unit = {
    val record = new ProducerRecord[String, String](topicName, "key", command.toString)
    producer.send(
      record,
      new Callback {
        override def onCompletion(
                                   metadata: RecordMetadata,
                                   exception: Exception
                                 ): Unit = {
          exception match {
            case e: Exception =>
            // todo
            // log errors
            case _ =>
              println(s"kafka published command: ${command.toString}")
          }
        }
      }
    )
  }
}
