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
  val topic = opt[String](required = true, descr = "the topic to consume from")
  val partition = opt[Int](default = Some(0), descr = "the partition to consume from")
  val offset = opt[Long](default = Some(-2L), descr = "the offset to start consuming from")
  val fetchSize = opt[Int](default = Some(1000000), descr = "the fetch size of each request")
  val printOffsets = opt[Boolean](descr = "print the offsets returned by the iterator")
  val lastOffset = opt[Boolean](descr = "print the last offset returned by the iterator")
  val countOnly = opt[Boolean](descr = "only print the total number of messages consumed")
  val server = opt[String](required = true, descr = "the hostname of the server to connect to", argName = "[kafka://]hostname:port")
  val socketTimeout = opt[Int](default = Some(10000), descr = "the socket timeout in seconds")
  val socketBufferSize = opt[Int](default = Some(64 * 1024), descr = "the socket buffer size in bytes")
  val charset = opt[String](default = Some("UTF-8"), descr = "message character set")
  val offsetSeparator = opt[String](default = Some("\01"), descr = "offset separator")
}

