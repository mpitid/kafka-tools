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

import org.apache.log4j.{ConsoleAppender, Level, Logger, SimpleLayout}


/** Command line program to dump out messages to standard out using the simple consumer. */
object run {

  def main(args: Array[String]): Unit = {
    // initialize Kafka's logging infrastructure to shut it up
    //System.setProperty("log4j.defaultInitOverride", "true")
    val logger = Logger.getRootLogger
    logger.addAppender(new ConsoleAppender(new SimpleLayout, ConsoleAppender.SYSTEM_ERR))
    logger.setLevel(Level.ERROR)
    new Consumer().consume(new Options(args))
  }
}
