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

import kafka.api.FetchRequest
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils


class Consumer {

  def consume(options: Options) {
    val server = options.server()
    val url = new URI(if (server.startsWith("kafka://")) server else "kafka://" + server)
    val topic = options.topic()
    val partition = options.partition()
    val startingOffset = options.offset()
    val fetchSize = options.fetchSize()
    val printOffsets = options.printOffsets()
    val offsetSeparator = options.offsetSeparator()
    val printLastOffset = options.lastOffset()
    val countOnly = options.countOnly()
    val charset = options.charset()

    val consumer = new SimpleConsumer(url.getHost, url.getPort, options.socketTimeout(), options.socketBufferSize())
    try {
      var offset =
        if (startingOffset < 0L) {
          consumer.getOffsetsBefore(topic, partition, startingOffset, 1).head
        } else startingOffset
      var consumed = 0L
      var hasMessages = true
      while (hasMessages) {
        val messages = consumer.fetch(new FetchRequest(topic, partition, offset, fetchSize))
        hasMessages = messages.nonEmpty
        for (messageAndOffset <- messages) {
          if (!countOnly) {
            if (printOffsets) {
              System.out.println(offset + offsetSeparator)
            }
            System.out.println(Utils.toString(messageAndOffset.message.payload, charset))
          }
          offset = messageAndOffset.offset
          consumed += 1
        }
      }
      if (printLastOffset) {
        System.err.println(offset)
      }
      if (countOnly) {
        System.out.println(consumed)
      }
    } finally consumer.close()
  }
}
