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
// See the NOTICE file distributed with this work for additional
// information regarding copyright ownership.

package kafka.tools.consumer

import java.net.URI
import java.nio.ByteBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.api._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.tools.consumer.Formatter.{FormatOptions, JsonFormatter, SimpleFormatter}

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal


class Consumer {
  import kafka.common.ErrorMapping.maybeThrowException
  private[this] val logger = org.log4s.getLogger

  def consume(options: Options) {
    val server = options.server()
    val url = new URI(if (server.startsWith("kafka://")) server else "kafka://" + server)
    val (host, port) = (url.getHost, url.getPort)
    val connectionString = s"$host:$port"
    val consumer = new SimpleConsumer(host, port, options.socketTimeout(), options.socketBufferSize(), options.clientId())
    val tap = topicPartition(options)
    try {
      if (options.fetchOffset() || options.commitOffset.get.nonEmpty) {
        val response = consumer.send(metadataRequest(options))
        maybeThrowException(response.errorCode)
        response.coordinatorOpt match {
          case Some(broker) =>
            val coordinator =
              if (broker.connectionString != connectionString) {
                new SimpleConsumer(broker.host, broker.port,options.socketTimeout(), options.socketBufferSize(), options.clientId())
              } else {
                consumer
              }
            commitRequest(options) match {
              case Some(r) =>
                val response = coordinator.commitOffsets(r)
                maybeThrowException(response.commitStatus(tap))
              case None =>
                val response = coordinator.fetchOffsets(offsetFetchRequest(options)).requestInfo(tap)
                maybeThrowException(response.error)
                println(response.offset)
            }
          case None =>
            throw new Exception(s"no offset coordinator for group ${options.groupId()}")
        }
      } else {
        val response = consumer.send(leaderRequest(options))
        val brokers = response.brokers
        response.topicsMetadata.collect {
          case tm if tm.topic == tap.topic =>
            maybeThrowException(tm.errorCode)
            tm.partitionsMetadata.filter(_.partitionId == options.partition())
        }.flatten.headOption match {
          case Some(pm) =>
            maybeThrowException(pm.errorCode)
            val broker = pm.leader.get
            val leader =
              if (broker.connectionString != options.server()) {
                new SimpleConsumer(broker.host, broker.port, options.socketTimeout(), options.socketBufferSize(), options.clientId())
              } else {
                consumer
              }
            offsetBeforeRequest(options).map { r =>
              val response = leader.getOffsetsBefore(r).partitionErrorAndOffsets(tap)
              maybeThrowException(response.error)
              println(response.offsets.head)
            }.getOrElse {
              options.offset.get match {
                case Some(offset) =>
                  if (!(options.keys() || options.values()
                    || options.offsets())) {
                    throw new Exception(s"nothing to do; run this program with --help for a list of options")
                  }
                  var hasMessages = true
                  var consumed = 0L
                  var currentOffset = offset
                  val formatOptions = FormatOptions(
                    options.jsonOffset.get.filter(_ => options.offsets())
                  , options.jsonKey.get.filter(_ => options.keys())
                  , options.jsonValue.get.filter(_ => options.values())
                  , options.charset(), options.fieldSeparator())
                  val formatter = if (options.json()) { new JsonFormatter(formatOptions) } else { new SimpleFormatter(formatOptions) }
                  while (!shouldStop(consumed, hasMessages)) {
                    val response = leader.fetch(fetchRequest(options, currentOffset)).data(tap)
                    maybeThrowException(response.error)
                    hasMessages = response.messages.nonEmpty
                    for (messageAndOffset <- response.messages) {
                      consumed += 1L
                      if (!shouldStop(consumed, hasMessages)) {
                        currentOffset = messageAndOffset.nextOffset
                        println(formatter.format(messageAndOffset))
                      }
                    }
                  }
                case None =>
                  throw new Exception(s"you need to specify a valid offset")
              }
            }
          case None =>
            throw new Exception(s"no leader information for partition ${options.partition()}")
        }
      }
      def shouldStop(consumed: Long, hasMessages: Boolean): Boolean = {
        options.messages.get match {
          case Some(max) => consumed >= max
          case None => !hasMessages
        }
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"fatal ${e.getClass.getName}: ${e.getMessage}")
        if (options.logErrors()) { logger.error(e)("fatal error") }
        System.exit(1)
    } finally consumer.close()
  }

  def offsetFetchRequest(o: Options): OffsetFetchRequest = {
    OffsetFetchRequest(o.groupId(), Seq(topicPartition(o)))
  }

  def fetchRequest(o: Options, offset: Long): FetchRequest = {
    FetchRequest(
      clientId = o.clientId()
    , requestInfo = Map(topicPartition(o) -> PartitionFetchInfo(offset, o.fetchSize()))
    )
  }

  def metadataRequest(o: Options): ConsumerMetadataRequest = {
    ConsumerMetadataRequest(
      o.groupId()
    , clientId = o.clientId()
    )
  }

  def offsetBeforeRequest(o: Options): Option[OffsetRequest] = {
    o.offsetBefore.get
     .orElse(when(o.latestOffset())(LatestOffset))
     .orElse(when(o.earliestOffset())(EarliestOffset)).map { ts =>
      OffsetRequest(
        Map(topicPartition(o) -> PartitionOffsetRequestInfo(ts, maxNumOffsets = 1))
      , clientId = o.clientId()
      )
    }
  }

  def commitRequest(o: Options): Option[OffsetCommitRequest] = {
    o.commitOffset.get.map { offset =>
      OffsetCommitRequest(
          o.groupId()
        , Map(topicPartition(o) -> OffsetAndMetadata(offset))
        , clientId = o.clientId()
      )
    }
  }

  def when[A](pred: => Boolean)(action: => A): Option[A] = {
    if (pred) { Some(action) } else { None }
  }

  def leaderRequest(o: Options): TopicMetadataRequest = {
    TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 1, o.clientId(), Seq(o.topic()))
  }

  def topicPartition(o: Options): TopicAndPartition = {
    TopicAndPartition(o.topic(), o.partition())
  }

  val LatestOffset = -1L
  val EarliestOffset = -2L
}

sealed trait Formatter {
  def format(maf: MessageAndOffset): String
}

object Formatter {
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
