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

import kafka.tools.Consumer.{FormatOptions, JsonFormatter, SimpleFormatter}
import kafka.tools.Producer.{JsonReader, ReaderOptions, SimpleReader, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{ConsoleAppender, Level, Logger, SimpleLayout}
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.util.control.NonFatal


object cli {

  private val logger = org.log4s.getLogger

  def main(args: Array[String]): Unit = {
    val opts = new Options(args)
    Logger.getRootLogger.addAppender(new ConsoleAppender(new SimpleLayout, ConsoleAppender.SYSTEM_ERR))
    Logger.getRootLogger.setLevel(Level.toLevel(opts.logLevel()))
    try {
      opts.subcommand match {
        case Some(c @ opts.push) =>
          val readOpts = ReaderOptions(
              c.jsonKey.get.filter(_ => c.keys())
            , c.jsonValue.get.filter(_ => c.values())
            , c.charset()
            , c.fieldSeparator()
            , c.ignoreMissing()
          )
          val reader = if (c.json()) { new JsonReader(readOpts) } else { new SimpleReader(readOpts) }
          val (host, port) = server(c.server())
          val prod = new Producer(Producer.Options(host, port, c.topic(), c.partition()
            , opts.soTimeout(), opts.soBufferSize(), opts.clientId(), c.messageSize()
            , c.acks(), c.codec(), c.retries(), c.retryBackoff(), c.topicRefresh()))
          prod.produce(reader)
        case Some(c @ opts.pull) =>
          val (host, port) = server(c.server())
          val options = Consumer.Options(host, port, c.topic(), c.partition(), opts.clientId(), opts.soTimeout(), opts.soBufferSize())
          val consumer = new Consumer(options)
          if (c.offset.isSupplied) {
            val formatOptions = FormatOptions(
              c.jsonOffset.get.filter(_ => c.offsets())
            , c.jsonKey.get.filter(_ => c.keys())
            , c.jsonValue.get.filter(_ => c.values())
            , c.charset()
            , c.fieldSeparator())
            val formatter = if (c.json()) { new JsonFormatter(formatOptions) } else { new SimpleFormatter(formatOptions) }
            consumer.fetchData(consumer.findLeader(options.simpleConsumer()), c.offset(), c.fetchSize(), formatter, c.messages.get, handler(c.messageSeparator()))
          } else {
            val offset =
              if (c.latestOffset()) {
                Consumer.LatestOffset
              } else if (c.earliestOffset()) {
                Consumer.EarliestOffset
              } else {
                c.offsetBefore()
              }
            println(consumer.fetchOffset(consumer.findLeader(options.simpleConsumer()), offset))
          }
        case Some(c @ opts.offsets) =>
          val (host, port) = server(c.server())
          val options = Consumer.Options(host, port, c.topic(), c.partition(), opts.clientId(), opts.soTimeout(), opts.soBufferSize())
          val offsets = new Offsets(Offsets.Options(c.groupId()), options)
          c.commit.get match {
            case Some(offset) =>
              offsets.commit(offsets.findCoordinator(options.simpleConsumer()), offset)
            case None =>
              println(offsets.fetch(offsets.findCoordinator(options.simpleConsumer())))
          }
        case Some(c @ opts.topics) =>
          val zk = new ZkClient(c.server(), c.sessionTimeout(), c.connectionTimeout(), ZKStringSerializer)
          val topics = new Topics()
          if (c.create() || c.update()) {
            topics.create(zk, c.topic(), c.partitions(), c.replicas(), c.minIsr.get, c.update())
          } else if (c.delete()) {
            topics.delete(zk, c.topic())
          } else if (c.info()) {
            val formatter = if (c.json()) { new Topics.JsonTopicsFormatter } else { new Topics.SimpleTopicsFormatter }
            for (info <- topics.details(zk, c.topic.get.toSet)) {
              println(formatter.format(info))
            }
          } else {
            for (topic <- topics.list(zk)) {
              println(topic)
            }
          }
          zk.close()
        case _ =>
          System.err.println(s"fatal: you must specify a sub-command")
          opts.printHelp()
          System.exit(2)
      }
    } catch {
      case NonFatal(e) =>
        logger.error(e)("fatal exception")
        System.err.println(s"fatal ${e.getClass.getName}: ${e.getMessage}")
        System.exit(1)
    }
  }

  def handler(separator: String): String => Unit = {
    s =>
      System.out.print(s)
      System.out.print(separator)
  }

  def server(s: String): (String, Int) = {
    val parts = s.split(':')
    (parts.init.mkString(":"), parts.last.toInt)
  }

  class Options(args: Seq[String]) extends ScallopConf(args) {
    version("kafka8-tools 1.0.0")
    banner(
      """
        |usage: kafka8-tools [OPTION] [push|pull|offsets|topics] [OPTION]
        |
        |Command line tools for working with Kafka 8
        |
        |Options:
      """.stripMargin)
    val push = new Subcommand("push") {
      val server = opt[String](required = true, short = 's', descr = "Kafka server to connect to", argName = "hostname:port")
      val topic = opt[String](required = true, short = 't', descr = "topic to write to", argName = "string")
      val partition = opt[Int](required = true, short = 'p', descr = "partition to write to", argName = "int")

      val acks = opt[Short](default = Some(0), short = 'a', validate = a => a >= -1 && a <= 1, descr = "kafka write ack policy 0: none, 1: leader, -1: ISR", argName = "int")

      val fieldSeparator = opt[String](default = Some("\01"), descr = "key/payload separator [\\01]", argName = "string")
      val keys = opt[Boolean](short = 'k', descr = "read key as first input field")
      val values = opt[Boolean](short = 'v', descr = "read payload as second input field")
      val charset = opt[String](default = Some("UTF-8"), descr = "specify character set for input file", argName = "string")
      val json = opt[Boolean](short = 'j', descr = "treat input as line-delimited JSON documents with: { key: string, payload: string }")
      val jsonKey = opt[String](default = Some("k"), descr = "set JSON field name for keys", argName = "string")
      val jsonValue = opt[String](default = Some("v"), descr = "set JSON field name for values", argName = "string")
      val ignoreMissing = opt[Boolean](short = 'i', descr = "treat missing JSON input fields as null")

      val messageSize = opt[Int](default = Some(1000000), validate = pos, descr = "max message size for each write to kafka", argName = "bytes")

      val codec = opt[Int](default = Some(0), validate = a => a >= 0 && a <= 3, descr = "kafka compression codec 0: none, 1: gzip, 2: snappy, 3: lz4", argName = "int")
      val retries = opt[Int](default = Some(0), validate = pos, descr = "number of retries on kafka write failure", argName = "int")
      val retryBackoff = opt[Int](default = Some(0), validate = pos, descr = "backoff between retries", argName = "ms")
      val topicRefresh = opt[Int](default = Some(60000), validate = pos, descr = "topic metadata refresh interval", argName = "ms")
      val help = opt[Boolean](short = 'h', descr = "show help message")
      mainOptions = Seq(server, topic, partition, acks, keys, values, json)
      validateOpt(keys, values) {
        case (Some(false), Some(false)) => Left("you must specify at least one of --keys or --values")
        case _ => Right(Unit)
      }
    }

    val pull = new Subcommand("pull") {
      val server = opt[String](required = true, short = 's', descr = "Kafka server to connect to", argName = "hostname:port")
      val topic = opt[String](required = true, short = 't', descr = "topic to read from", argName = "string")
      val partition = opt[Int](required = true, short = 'p', descr = "partition to read from", argName = "int")

      val offset = opt[Long](short = 'o', descr = "offset to start consuming from", argName = "int")
      val offsetBefore = opt[Long](short = 'b', descr = "fetch last offset before a given timestamp", argName = "ms")
      val latestOffset = opt[Boolean](short = 'l', descr = "fetch latest offset")
      val earliestOffset = opt[Boolean](short = 'e', descr = "fetch earliest offset")

      val fetchSize = opt[Int](default = Some(1000000), descr = "fetch size of each request", argName = "bytes")
      val messages = opt[Long](short = 'm', descr = "block until this number of messages has been consumed")

      val offsets = opt[Boolean](descr = "print offset for each message")
      val keys = opt[Boolean](short = 'k', descr = "print key for each message")
      val values = opt[Boolean](short = 'v', descr = "print value (payload) for each message")

      val charset = opt[String](default = Some("UTF-8"), descr = "key/value character set")
      val fieldSeparator = opt[String](default = Some("\01"), descr = "offset/key/value separator [\\01]")
      val messageSeparator = opt[String](default = Some("\n"), descr = "message separator [\\n]")
      val json = opt[Boolean](short = 'j', descr = "structure output as JSON documents: { $offset: long, $key: string, $value: string }")
      val jsonKey = opt[String](default = Some("k"), descr = "set JSON field name for keys", argName = "string")
      val jsonValue = opt[String](default = Some("v"), descr = "set JSON field name for values", argName = "string")
      val jsonOffset = opt[String](default = Some("o"), descr = "set JSON field name for offsets", argName = "string")
      val help = opt[Boolean](short = 'h', descr = "show help message")

      requireOne(offset, offsetBefore, latestOffset, earliestOffset)
      validateOpt(offset, offsets, keys, values) {
        case (Some(_), Some(false), Some(false), Some(false)) => Left("you must specify one of --offsets, --keys or --values with --offset")
        case _ => Right(Unit)
      }
      mainOptions = Seq(server, topic, partition, offset, latestOffset, messages, offsets, keys, values)
    }

    val offsets = new Subcommand("offsets") {
      val server = opt[String](required = true, short = 's', descr = "Kafka server to connect to", argName = "hostname:port")
      val topic = opt[String](required = true, short = 't', descr = "topic to fetch/commit offset for", argName = "string")
      val partition = opt[Int](required = true, short = 'p', descr = "partition to fetch/commit offset for", argName = "int")
      val groupId = opt[String](required = true, short = 'g', default = Some("cli"), descr = "consumer group to fetch/commit offset for", argName = "string")

      val commit = opt[Long](short = 'c', descr = "commit an offset for a topic/partition/group combination", argName = "long")
      val help = opt[Boolean](short = 'h', descr = "show help message")
      //val offsetRetention = opt[Int]() 0.8.3
      mainOptions = Seq(server, topic, partition, groupId, commit)
    }

    val topics = new Subcommand("topics") {
      footer("    if no action is specified list topics")
      val server = opt[String](required = true, short = 's', descr = "ZooKeeper server to connect to", argName = "hostname:port")
      val topic = opt[String](short = 't', descr = "topic to create/update", argName = "string")
      val partitions = opt[Int](short = 'p', descr = "number of partitions", argName = "int")
      val replicas = opt[Int](short = 'r', validate = _ >= 1, descr = "replication factor", argName = "int")
      val minIsr = opt[Int](short = 'm', validate = pos, descr = "minimum number of nodes in ISR for a topic", argName = "int")

      val create = opt[Boolean](short = 'c', descr = "create topic")
      val update = opt[Boolean](short = 'u', descr = "update topic metadata")
      val delete = opt[Boolean](short = 'd', descr = "delete topic")
      val info = opt[Boolean](short = 'i', descr = "detailed topic information")

      val json = opt[Boolean](short = 'j', descr = "format topic information as JSON")

      val sessionTimeout = opt[Int](default = Some(10000), validate = pos, descr = "ZooKeeper session timeout", argName = "ms")
      val connectionTimeout = opt[Int](default = Some(10000), validate = pos, descr = "ZooKeeper connection timeout", argName = "ms")
      val help = opt[Boolean](short = 'h', descr = "show help message")

      dependsOnAll(create, List(topic, partitions, replicas))
      dependsOnAll(update, List(topic))
      dependsOnAll(delete, List(topic))
      val commands = Seq(create, update, delete, info)
      for (i <- 0 until commands.size; j <- (i+1) until commands.size) {
        mutuallyExclusive(commands(i), commands(j))
      }
      mainOptions = Seq(server, topic, partitions, replicas, minIsr, update)
    }

    val clientId = opt[String](short = 'c', default = Some(""), descr = "client ID send to Kafka brokers", argName = "string")
    val soTimeout = opt[Int](short = 't', default = Some(10000), descr = "socket timeout", argName = "ms")
    val soBufferSize = opt[Int](short = 'b', default = Some(64 * 1024), descr = "socket buffer size", argName = "bytes")
    val logLevel = opt[String](short = 'l', descr = "set log level", default = Some("off"))
    val help = opt[Boolean](short = 'h', descr = "show help message")
    val version = opt[Boolean](descr = "show version of this program")
    def pos: Int => Boolean = { _ >= 0 }
  }
}
