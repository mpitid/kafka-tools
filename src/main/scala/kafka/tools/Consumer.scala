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

import java.nio.ByteBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.api._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.tools.Consumer.ConsumerException

import scala.collection.mutable.ListBuffer


class Consumer(val options: Consumer.Options) {

  import kafka.common.ErrorMapping.maybeThrowException

  def fetchData(leader: SimpleConsumer, offset: Consumer.Offset, fetchSize: Int, formatter: Consumer.Formatter, maxMessages: Option[Long], handler: String => Unit) = {
    var hasMessages = true
    var consumed = 0L
    var currentOffset = offset
    while (!shouldStop(consumed, hasMessages, maxMessages)) {
      val response = leader.fetch(fetchRequest(currentOffset, fetchSize)).data(options.tap)
      maybeThrowException(response.error)
      hasMessages = response.messages.nonEmpty
      for (messageAndOffset <- response.messages) {
        consumed += 1L
        if (!shouldStop(consumed, hasMessages, maxMessages)) {
          currentOffset = messageAndOffset.nextOffset
          handler(formatter.format(messageAndOffset))
        }
      }
    }
  }

  protected def shouldStop(consumed: Long, hasMessages: Boolean, maxMessages: Option[Long]): Boolean = {
    maxMessages match {
      case Some(max) => consumed >= max
      case None => !hasMessages
    }
  }

  def fetchOffset(leader: SimpleConsumer, offset: Consumer.Offset): Consumer.Offset = {
    val response = leader.getOffsetsBefore(offsetBeforeRequest(offset)).partitionErrorAndOffsets(options.tap)
    maybeThrowException(response.error)
    response.offsets.head
  }

  def findLeader(consumer: SimpleConsumer): SimpleConsumer = {
    val response = consumer.send(leaderRequest())
    val brokers = response.brokers
    response.topicsMetadata.collect {
      case tm if tm.topic == options.topic =>
        maybeThrowException(tm.errorCode)
        tm.partitionsMetadata.filter(_.partitionId == options.partition)
    }.flatten.headOption match {
      case Some(pm) =>
        maybeThrowException(pm.errorCode)
        val broker = pm.leader.get
        if (broker.connectionString != options.connection) {
          new SimpleConsumer(broker.host, broker.port, options.soTimeout, options.soBufferSize, options.clientId)
        } else {
          consumer
        }
      case None =>
        throw ConsumerException(s"no leader information for topic ${options.topic} partition ${options.partition}")
    }
  }

  def fetchRequest(offset: Consumer.Offset, fetchSize: Int): FetchRequest = {
    FetchRequest(clientId = options.clientId, requestInfo = Map(options.tap -> PartitionFetchInfo(offset, fetchSize)))
  }

  def offsetBeforeRequest(o: Consumer.Offset): OffsetRequest = {
    OffsetRequest(Map(options.tap -> PartitionOffsetRequestInfo(o, maxNumOffsets = 1)), clientId = options.clientId)
  }

  def leaderRequest(): TopicMetadataRequest = {
    TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 1, options.clientId, Seq(options.topic))
  }
}

object Consumer {
  val LatestOffset = -1L
  val EarliestOffset = -2L

  case class ConsumerException(message: String) extends Exception(message)

  type Offset = Long
  case class Options(host: String, port: Int, topic: String, partition: Int, clientId: String, soTimeout: Int, soBufferSize: Int) {
    def connection = { s"$host:$port" }
    def tap = { TopicAndPartition(topic, partition) }
    def simpleConsumer(h: Option[String] = None, p: Option[Int] = None) = {
      new SimpleConsumer(h.getOrElse(host), p.getOrElse(port), soTimeout, soBufferSize, clientId)
    }
  }

  sealed trait Formatter {
    def format(maf: MessageAndOffset): String
  }

  case class FormatOptions(offsets: Option[String], keys: Option[String], values: Option[String], charset: String, separator: String)
  class JsonFormatter(options: FormatOptions) extends Formatter {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    def format(maf: MessageAndOffset): String = {
      val obj = JsonNodeFactory.instance.objectNode()
      options.offsets.foreach(k => obj.put(k, maf.offset))
      options.keys.foreach(k => obj.put(k, extract(maf.message.key, options.charset)))
      options.values.foreach(k => obj.put(k, extract(maf.message.payload, options.charset)))
      mapper.writeValueAsString(obj)
    }
  }
  class SimpleFormatter(options: FormatOptions) extends Formatter {
    def format(maf: MessageAndOffset): String = {
      val buffer = ListBuffer[String]()
      options.offsets.foreach { _ => buffer.append(maf.offset.toString) }
      options.keys.foreach { _ => buffer.append(Option(extract(maf.message.key, options.charset)).getOrElse("")) }
      options.values.foreach { _ => buffer.append(Option(extract(maf.message.payload, options.charset)).getOrElse("")) }
      buffer.mkString(options.separator)
    }
  }
  def extract(buf: ByteBuffer, charset: String): String = {
    Option(buf).map { buffer =>
      val bytes = new Array[Byte](buffer.remaining)
      buffer.get(bytes)
      new String(bytes, charset)
    }.orNull
  }
}


class Offsets(val options: Offsets.Options, val consumerOptions: Consumer.Options) {

  import kafka.common.ErrorMapping.maybeThrowException

  def findCoordinator(consumer: SimpleConsumer) = {
    val response = consumer.send(metadataRequest())
    maybeThrowException(response.errorCode)
    response.coordinatorOpt match {
      case Some(broker) =>
        if (broker.connectionString == consumerOptions.connection) {
          consumer
        } else {
          consumerOptions.simpleConsumer(Some(broker.host), Some(broker.port))
        }
      case None =>
        throw ConsumerException(s"no offset coordinator for group ${options.groupId}")
    }
  }

  def commit(consumer: SimpleConsumer, offset: Consumer.Offset) = {
    val response = consumer.commitOffsets(commitRequest(offset))
    maybeThrowException(response.commitStatus(consumerOptions.tap))
  }

  def fetch(consumer: SimpleConsumer): Consumer.Offset = {
    val response = consumer.fetchOffsets(fetchRequest()).requestInfo(consumerOptions.tap)
    maybeThrowException(response.error)
    response.offset
  }

  def fetchRequest() = {
    OffsetFetchRequest(options.groupId, Seq(consumerOptions.tap))
  }

  def commitRequest(offset: Consumer.Offset) = {
    OffsetCommitRequest(options.groupId, Map(consumerOptions.tap -> OffsetAndMetadata(offset)), clientId = consumerOptions.clientId)
  }

  def metadataRequest(): ConsumerMetadataRequest = {
    ConsumerMetadataRequest(options.groupId, clientId = consumerOptions.clientId)
  }
}

object Offsets {
  case class Options(groupId: String)
}

