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

import org.rogach.scallop.ScallopConf

class Options(args: Seq[String]) extends ScallopConf(args) {

  val server = opt[String](required = true, short = 's',  descr = "the hostname of the server to connect to", argName = "hostname:port")
  val topic = opt[String](required = true, short = 't', descr = "the topic to write to", argName = "string")
  val partition = opt[Int](required = true, short = 'p', validate = pos, descr = "the partition to write to or the number of partitions for a topic", argName = "int")

  val acks = opt[Short](default = Some(0), short = 'a', validate = a => a >= -1 && a <= 1, descr = "kafka write ack policy 0: none, 1: leader, -1: ISR", argName = "int")

  val createTopic = opt[Boolean](short = 'c', descr = "create a new topic")
  val updateTopic = opt[Boolean](short = 'u', descr = "update a topic")
  val minIsr = opt[Int](short = 'm', validate = pos, descr = "minimum number of nodes in the ISR for a topic", argName = "int")
  val replicas = opt[Int](short = 'r', validate = _ >= 1, descr = "number of replicas for a topic", argName = "int")

  val zkSessionTimeout = opt[Int](default = Some(10000), validate = pos, descr = "zookeeper session timeout", argName = "ms")
  val zkConnectionTimeout = opt[Int](default = Some(10000), validate = pos, descr = "zookeeper connection timeout", argName = "ms")

  val fieldSeparator = opt[String](default = Some("\01"), descr = "key/payload separator [\\01]", argName = "string")
  val keys = opt[Boolean](short = 'k', descr = "read key as the first input field")
  val values = opt[Boolean](short = 'v', descr = "read payload as the second input field")
  val charset = opt[String](default = Some("UTF-8"), descr = "specify character set for input file", argName = "string")
  val json = opt[Boolean](short = 'j', descr = "treat input as line-delimited JSON documents with: { key: string, payload: string }")
  val jsonKey = opt[String](default = Some("k"), descr = "set the JSON field name for keys", argName = "string")
  val jsonValue = opt[String](default = Some("v"), descr = "set the JSON field name for values", argName = "string")
  val ignoreMissing = opt[Boolean](descr = "treat missing JSON input fields as null")

  val messageSize = opt[Int](default = Some(1000000), validate = pos, descr = "max message size for each write to kafka", argName = "bytes")
  val socketTimeout = opt[Int](default = Some(10000), validate = pos, descr = "kafka socket timeout when sending data", argName = "ms")
  val socketBufferSize = opt[Int](default = Some(64 * 1024), validate = pos, descr = "the socket buffer size", argName = "bytes")

  val clientId = opt[String](default = Some(""), descr = "client ID sent to kafka", argName = "string")
  val codec = opt[Int](default = Some(0), validate = a => a >= 0 && a <= 3, descr = "kafka compression codec 0: none, 1: gzip, 2: snappy, 3: lz4", argName = "int")
  val retries = opt[Int](default = Some(0), validate = pos, descr = "number of retries on kafka write failure", argName = "int")
  val retryBackoff = opt[Int](default = Some(0), validate = pos, descr = "backoff between retries", argName = "ms")
  val topicRefresh = opt[Int](default = Some(60000), validate = pos, descr = "topic metadata refresh interval", argName = "ms")

  val logErrors = opt[Boolean](descr = "log stack traces in case of errors")

  def pos: Int => Boolean = { _ >= 0 }
}

