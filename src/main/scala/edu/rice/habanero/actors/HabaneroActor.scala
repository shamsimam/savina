package edu.rice.habanero.actors

import edu.rice.hj.runtime.actors.Actor

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
abstract class HabaneroActor[MsgType](seqBody: Boolean = true) extends Actor[MsgType](seqBody) {

  override def process(msg: MsgType): Unit

  override def handleThrowable(th: Throwable): Unit = {
    th.printStackTrace()
  }
}
