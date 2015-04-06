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

import java.net.URI

import kafka.message.{ByteBufferMessageSet, CompressionCodec, Message}
import kafka.producer.{SyncProducer, SyncProducerConfig}

import collection.mutable.ListBuffer

class Producer {

  type Bytes = Array[Byte]

  def produce(options: Options): Unit = {
    val server = options.server()
    val url = new URI(if (server.startsWith("kafka://")) server else "kafka://" + server)
    val topic = options.topic()

    val properties = props(
      "host" -> url.getHost
    , "port" -> url.getPort
    , "buffer.size" -> options.socketBufferSize()
    , "socket.timeout.ms" -> options.socketTimeout()
    , "connect.timeout.ms" -> options.connectTimeout()
    , "reconnect.interval" -> options.reconnectInterval()
    , "reconnect.time.interval.ms" -> options.reconnectTimeInterval()
    , "max.message.size" -> options.messageSize()
    )

    val producer = new SyncProducer(new SyncProducerConfig(properties))
    try {
      val send = options.partition.get match {
        case Some(p) => producer.send(topic, p, _: ByteBufferMessageSet)
        case None => producer.send(topic, _: ByteBufferMessageSet)
      }

      val codec = CompressionCodec.getCompressionCodec(options.codec())
      def encode(data: Seq[Bytes]) = {
        new ByteBufferMessageSet(codec, data.map(new Message(_)): _*)
      }

      val batch = new ListBuffer[Bytes]
      var size = 0L
      for (line <- scala.io.Source.stdin.getLines()) {
        val bytes = line.getBytes(options.charset())
        size += bytes.size
        if (size >= options.messageSize()) {
          size = bytes.size
          send(encode(batch))
          batch.clear()
        }
        batch.append(bytes)
      }
      if (batch.nonEmpty) {
        send(encode(batch))
      }
    } finally producer.close()
  }

  def props(items: (String, Any)*): java.util.Properties = {
    val p = new java.util.Properties()
    for ((k, v) <- items)
      p.setProperty(k, v.toString)
    p
  }
}
