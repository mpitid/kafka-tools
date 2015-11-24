// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301 USA
//
// Copyright 2015 Michael Pitidis

package kafka.tools

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, ProducerConfig}
import kafka.tools.Producer.Bytes
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

import scala.collection.mutable.ListBuffer

class Topics() {
  def createTopic(zk: ZkClient, topic: String, partitions: Int, replicas: Int, minISR: Option[Int], update: Boolean) = {
    val brokers = ZkUtils.getSortedBrokerList(zk)
    val assignment = AdminUtils.assignReplicasToBrokers(brokers, partitions, replicas)
    val config = Producer.properties(minISR.map("min.insync.replicas".->).toSeq: _*)
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zk, topic, assignment, config, update = update)
  }
}

class Producer(options: Producer.Options) {

  def produce(reader: Producer.Reader): Unit = {
    val producer = complexProducer()
    try {
      val batch = new ListBuffer[(Option[Bytes], Option[Bytes])]
      var size = 0L
      for (line <- scala.io.Source.stdin.getLines()) {
        val bytes = reader.read(line)
        val entrySize = bytes._1.map(_.length).getOrElse(0) + bytes._2.map(_.length).getOrElse(0)
        size += entrySize
        if (size >= options.messageSize) {
          size = entrySize
          producer.send(encode(batch): _*)
          batch.clear()
        }
        batch.append(bytes)
      }
      if (batch.nonEmpty) {
        producer.send(encode(batch): _*)
      }
    } finally producer.close()
  }

  def complexProducer(): kafka.producer.Producer[Bytes, Bytes] = {
    new kafka.producer.Producer[Bytes, Bytes](new ProducerConfig(options.configuration()))
  }

  def encode(data: Seq[(Option[Bytes], Option[Bytes])]) = {
    data.map {
      case (k, v) => KeyedMessage(options.topic, k.orNull, options.partition, v.orNull)
    }
  }
}

object Producer {
  type Bytes = Array[Byte]

  case class Options(host: String, port: Int, topic: String, partition: Int, soTimeout: Int, soBufferSize: Int, clientId: String, messageSize: Int, acks: Short, codec: Int, retries: Int, retryBackoff: Int, topicRefresh: Int) {
    def configuration() = {
      properties(
        "metadata.broker.list" -> s"$host:$port"
      , "message.send.max.retries" -> retries
      , "retry.backoff.ms" -> retryBackoff
      , "client.id" -> clientId
      , "send.buffer.bytes" -> soBufferSize
      , "request.timeout.ms" -> soTimeout
      , "request.required.acks" -> acks
      , "topic.metadata.refresh.interval.ms" -> topicRefresh
      , "producer.type" -> "sync"
      , "compression.codec" -> codec
      //, "min.isr" -> 2 // This is ignored due to a bug in Kafka auto-creation.
      )
    }
  }

  sealed trait Reader {
    def read(entry: String): (Option[Bytes], Option[Bytes])
  }

  case class ReaderOptions(keys: Option[String], values: Option[String], charset: String, separator: String, ignoreMissing: Boolean)

  class SimpleReader(options: ReaderOptions) extends Reader {
    def read(entry: String): (Option[Bytes], Option[Bytes]) = {
      (options.keys, options.values) match {
        case (Some(_), Some(_)) =>
          entry.split(options.separator, 2) match {
            case Array(key, payload) =>
              (Some(key.getBytes(options.charset)), Some(payload.getBytes(options.charset)))
            case _ =>
              throw new Exception(s"could not extract key and payload from message with separator `${options.separator}`")
          }
        case _ =>
          (options.keys.map(_.getBytes(options.charset)), options.values.map(_.getBytes(options.charset)))
      }
    }
  }

  class JsonReader(options: ReaderOptions) extends Reader {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    def read(entry: String): (Option[Bytes], Option[Bytes]) = {
      val doc = mapper.readValue(entry, classOf[JsonNode])
      (options.keys.flatMap(extract(doc, _)), options.values.flatMap(extract(doc, _)))
    }
    protected def extract(doc: JsonNode, field: String): Option[Bytes] = {
      Option(doc.path(field).asText(null)).map(_.getBytes(options.charset)).ensuring(options.ignoreMissing || _.nonEmpty, s"missing input field `$field`")
    }
  }

  object ZKStringSerializer extends ZkSerializer {
    @throws(classOf[ZkMarshallingError])
    def serialize(data : Object) : Array[Byte] = { data.asInstanceOf[String].getBytes("UTF-8") }
    @throws(classOf[ZkMarshallingError])
    def deserialize(bytes : Array[Byte]) : Object = { Option(bytes).map(b => new String(b, "UTF-8")).orNull }
  }

  def properties(items: (String, Any)*): java.util.Properties = {
    val p = new java.util.Properties()
    for ((k, v) <- items)
      p.setProperty(k, v.toString)
    p
  }
}

