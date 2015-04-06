
package kafka.tools.producer

import org.apache.log4j.{ConsoleAppender, Level, Logger, SimpleLayout}


/** Command line program to dump message from standard input using the simple produer. */
object run {

  def main(args: Array[String]): Unit = {
    // initialize Kafka's logging infrastructure to shut it up
    //System.setProperty("log4j.defaultInitOverride", "true")
    val logger = Logger.getRootLogger
    logger.addAppender(new ConsoleAppender(new SimpleLayout, ConsoleAppender.SYSTEM_ERR))
    logger.setLevel(Level.ERROR)
    new Producer().produce(new Options(args))
  }

}
