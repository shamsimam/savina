package edu.rice.habanero.actors

import java.util.function.Predicate

import edu.rice.hj.experimental.actors.{DeclarativeSelector, Selector}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */

abstract class HabaneroSelector[MsgType](val numMailboxes: Int, alwaysSeq: Boolean = true, usePriorities: Boolean = true)
  extends Selector[MsgType](numMailboxes, alwaysSeq, usePriorities) {

  override def process(msg: MsgType): Unit

}

abstract class HabaneroDeclarativeSelector[MsgType](numMailboxes: Int, alwaysSeq: Boolean = true, usePriorities: Boolean = true)
  extends DeclarativeSelector[MsgType](numMailboxes, alwaysSeq, usePriorities) {

  override def registerGuards(): Unit

  override def doProcess(msg: MsgType): Unit

  def guard(mailbox: Enum[_], p: (MsgType) => Boolean): Unit = {
    guard(mailbox.ordinal(), p)
  }

  def guard(mailboxId: Int, p: (MsgType) => Boolean): Unit = {
    guard(mailboxId, new Predicate[MsgType] {
      override def test(msg: MsgType) = p.apply(msg)
    })
  }

}
