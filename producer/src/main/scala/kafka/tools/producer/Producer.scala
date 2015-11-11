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

package kafka.tools.producer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, ProducerConfig}
import kafka.tools.producer.Reader.{ReaderOptions, Bytes, JsonReader, SimpleReader}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

class Producer {
  private[this] val logger = org.log4s.getLogger

  def encode(topic: String, partition: Int, data: Seq[(Option[Bytes], Option[Bytes])]) = {
    data.map {
      case (k, v) => KeyedMessage(topic, k.orNull, partition, v.orNull)
    }
  }

  def produce(options: Options): Unit = {
    val server = options.server().replace("kafka://", "").replace("zookeeper://", "")
    val topic = options.topic()
    val partition = options.partition()

    val properties = props(
      "metadata.broker.list" -> server
    , "message.send.max.retries" -> options.retries()
    , "retry.backoff.ms" -> options.retryBackoff()
    , "client.id" -> options.clientId()
    , "send.buffer.bytes" -> options.socketBufferSize()
    , "request.timeout.ms" -> options.socketTimeout()
    , "request.required.acks" -> options.acks()
    , "topic.metadata.refresh.interval.ms" -> options.topicRefresh()
    , "producer.type" -> "sync"
    , "compression.codec" -> options.codec()
    //, "min.isr" -> 2 // This is ignored due to a bug in Kafka auto-creation.
    )

    val producer = new kafka.producer.Producer[Bytes, Bytes](new ProducerConfig(properties))
    try {
      if (!(options.keys() || options.values() || options.createTopic() || options.updateTopic())) {
        throw new Exception(s"nothing to do; run this program with --help for a list of options")
      }

      if (options.createTopic() || options.updateTopic()) {
        if (options.replicas.isEmpty) {
          throw new Exception(s"you need to specify a replication factor when creating or updating a topic")
        }
        val zk = new ZkClient(server, options.zkSessionTimeout(), options.zkConnectionTimeout(), ZKStringSerializer)
        createTopic(zk, topic, partition, options.replicas(), options.minIsr.get, options.updateTopic())
      } else {
        val readOpts = ReaderOptions(
          options.jsonKey.get.filter(_ => options.keys())
        , options.jsonValue.get.filter(_ => options.values())
        , options.charset()
        , options.fieldSeparator()
        , options.ignoreMissing()
        )
        val reader = if (options.json()) { new JsonReader(readOpts) } else { new SimpleReader(readOpts) }
        val batch = new ListBuffer[(Option[Bytes], Option[Bytes])]
        var size = 0L
        for (line <- scala.io.Source.stdin.getLines()) {
          val bytes = reader.read(line)
          val entrySize = bytes._1.map(_.length).getOrElse(0) + bytes._2.map(_.length).getOrElse(0)
          size += entrySize
          if (size >= options.messageSize()) {
            size = entrySize
            producer.send(encode(topic, partition, batch): _*)
            batch.clear()
          }
          batch.append(bytes)
        }
        if (batch.nonEmpty) {
          producer.send(encode(topic, partition, batch): _*)
        }
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"fatal ${e.getClass.getName}: ${e.getMessage}")
        if (options.logErrors()) { logger.error(e)("fatal error") }
        System.exit(1)
    } finally producer.close()
  }

  def createTopic(zk: ZkClient, topic: String, partitions: Int, replicas: Int, minISR: Option[Int], update: Boolean) = {
    val brokers = ZkUtils.getSortedBrokerList(zk)
    val assignment = AdminUtils.assignReplicasToBrokers(brokers, partitions, replicas)
    val config = minISR.map(m => props(s"min.insync.replicas" -> m)).getOrElse(props())
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zk, topic, assignment, config, update = update)
  }

  def props(items: (String, Any)*): java.util.Properties = {
    val p = new java.util.Properties()
    for ((k, v) <- items)
      p.setProperty(k, v.toString)
    p
  }
}

sealed trait Reader {
  def read(entry: String): (Option[Bytes], Option[Bytes])
}

object Reader {
  type Bytes = Array[Byte]
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
}

object ZKStringSerializer extends ZkSerializer {
  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = { data.asInstanceOf[String].getBytes("UTF-8") }
  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = { Option(bytes).map(b => new String(b, "UTF-8")).orNull }
}
