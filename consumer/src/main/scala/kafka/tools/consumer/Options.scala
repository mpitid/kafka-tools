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

package kafka.tools.consumer

import org.rogach.scallop.ScallopConf

class Options(args: Seq[String]) extends ScallopConf(args) {

  val server = opt[String](required = true, short = 's', descr = "the hostname of the server to connect to", argName = "hostname:port")
  val topic = opt[String](required = true, short = 't', descr = "the topic to consume from", argName = "string")
  val partition = opt[Int](required = true, short = 'p', descr = "the partition to consume from", argName = "int")
  val offset = opt[Long](short = 'o', descr = "the offset to start consuming from", argName = "int")

  val groupId = opt[String](short = 'g', default = Some("cli"), descr = "the consumer group to store or fetch offsets from", argName = "string")
  val commitOffset = opt[Long](short = 'c', descr = "commit an offset for a topic/partition/group combination", argName = "long")
  val fetchOffset = opt[Boolean](short = 'f', descr = "fetch a stored offset for a topic/partition/group combination")

  val offsetBefore = opt[Long](short = 'b', descr = "fetch last offset before a given timestamp", argName = "ms")
  val latestOffset = opt[Boolean](short = 'l', descr = "fetch latest offset")
  val earliestOffset = opt[Boolean](short = 'e', descr = "fetch earliest offset")

  //  val offsetRetention = opt[Int]() 0.8.3
  val clientId = opt[String](default = Some(""), descr = "client ID send to broker", argName = "string")
  val fetchSize = opt[Int](default = Some(1000000), descr = "the fetch size of each request", argName = "bytes")
  val socketTimeout = opt[Int](default = Some(10000), descr = "the socket timeout in seconds", argName = "ms")
  val socketBufferSize = opt[Int](default = Some(64 * 1024), descr = "the socket buffer size", argName = "bytes")

  val messages = opt[Long](short = 'm', descr = "block until this number of messages has been consumed")

  val offsets = opt[Boolean](descr = "print the offset for each message")
  val keys = opt[Boolean](short = 'k', descr = "print the key for each message")
  val values = opt[Boolean](short = 'v', descr = "print the value (payload) for each message")

  val charset = opt[String](default = Some("UTF-8"), descr = "key/value character set")
  val fieldSeparator = opt[String](default = Some("\01"), descr = "offset/key/value separator [\\01]")
  val messageSeparator = opt[String](default = Some("\n"), descr = "message separator [\\n]")
  val json = opt[Boolean](short = 'j', descr = "structure output as JSON documents: { $offset: long, $key: string, $value: string }")
  val jsonKey = opt[String](default = Some("k"), descr = "set the JSON field name for keys", argName = "string")
  val jsonValue = opt[String](default = Some("v"), descr = "set the JSON field name for values", argName = "string")
  val jsonOffset = opt[String](default = Some("o"), descr = "set the JSON field name for offsets", argName = "string")
  val logErrors = opt[Boolean](descr = "log stack traces in case of errors")
}

