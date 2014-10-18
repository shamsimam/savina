package akka.actor

import akka.dispatch.{DequeBasedMessageQueueSemantics, Envelope, Mailboxes, RequiresMessageQueue}

import scala.collection.immutable


trait CustomStash extends CustomUnrestrictedStash with RequiresMessageQueue[DequeBasedMessageQueueSemantics]

/**
 * A version of [[akka.actor.Stash]] that does not enforce any mailbox type. The proper mailbox has to be configured
 * manually, and the mailbox should extend the [[akka.dispatch.DequeBasedMessageQueueSemantics]] marker trait.
 */
trait CustomUnrestrictedStash extends Actor with CustomStashSupport {
  /**
   * Overridden callback. Prepends all messages in the stash to the mailbox,
   * clears the stash, stops all children and invokes the postStop() callback.
   */
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    try unstashAll() finally super.preRestart(reason, message)
  }

  /**
   * Overridden callback. Prepends all messages in the stash to the mailbox and clears the stash.
   * Must be called when overriding this method, otherwise stashed messages won't be propagated to DeadLetters
   * when actor stops.
   */
  override def postStop(): Unit = try unstashAll() finally super.postStop()
}

/**
 * INTERNAL API.
 *
 * Support trait for implementing a stash for an actor instance. A default stash per actor (= user stash)
 * is maintained by [[UnrestrictedStash]] by extending this trait. Actors that explicitly need other stashes
 * (optionally in addition to and isolated from the user stash) can create new stashes via [[StashFactory]].
 */
private[akka] trait CustomStashSupport {
  /**
   * INTERNAL API.
   *
   * Context of the actor that uses this stash.
   */
  private[akka] def context: ActorContext

  /**
   * INTERNAL API.
   *
   * Self reference of the actor that uses this stash.
   */
  private[akka] def self: ActorRef

  /* The private stash of the actor. It is only accessible using `stash()` and
   * `unstashAll()`.
   */
  private var theStash = Vector.empty[Envelope]

  private def actorCell = context.asInstanceOf[ActorCell]

  /* The capacity of the stash. Configured in the actor's mailbox or dispatcher config.
   */
  private val capacity: Int = {
    val dispatcher = context.system.settings.config.getConfig(context.props.dispatcher)
    val fallback = dispatcher.withFallback(context.system.settings.config.getConfig(Mailboxes.DefaultMailboxId))
    val config =
      if (context.props.mailbox == Mailboxes.DefaultMailboxId) fallback
      else context.system.settings.config.getConfig(context.props.mailbox).withFallback(fallback)
    config.getInt("stash-capacity")
  }

  /**
   * INTERNAL API.
   *
   * The actor's deque-based message queue.
   * `mailbox.queue` is the underlying `Deque`.
   */
  private[akka] val mailbox: DequeBasedMessageQueueSemantics = {
    actorCell.mailbox.messageQueue match {
      case queue: DequeBasedMessageQueueSemantics ⇒ queue
      case other ⇒ throw ActorInitializationException(self, s"DequeBasedMailbox required, got: ${other.getClass.getName}\n" +
        """An (unbounded) deque-based mailbox can be configured as follows:
          |  my-custom-mailbox {
          |    mailbox-type = "akka.dispatch.UnboundedDequeBasedMailbox"
          |  }
          | """.stripMargin)
    }
  }

  /**
   * Adds the current message (the message that the actor received last) to the
   * actor's stash.
   *
   * @throws StashOverflowException in case of a stash capacity violation
   * @throws IllegalStateException  if the same message is stashed more than once
   */
  def stash(): Unit = {
    val currMsg = actorCell.currentMessage
    if (theStash.nonEmpty && (currMsg eq theStash.last))
      throw new IllegalStateException("Can't stash the same message " + currMsg + " more than once")
    if (capacity <= 0 || theStash.size < capacity) theStash :+= currMsg
    else throw new StashOverflowException("Couldn't enqueue message " + currMsg + " to stash of " + self)
  }

  /**
   * Prepends `others` to this stash. This method is optimized for a large stash and
   * small `others`.
   */
  private[akka] def prepend(others: immutable.Seq[Envelope]): Unit =
    theStash = others.foldRight(theStash)((e, s) ⇒ e +: s)

  /**
   * Notifies if the stash is empty
   */
  def stashNonEmpty(): Boolean = theStash.nonEmpty

  /**
   * Prepends the oldest message in the stash to the mailbox, and then removes that
   * message from the stash.
   *
   * Messages from the stash are enqueued to the mailbox until the capacity of the
   * mailbox (if any) has been reached. In case a bounded mailbox overflows, a
   * `MessageQueueAppendFailedException` is thrown.
   *
   * The unstashed message is guaranteed to be removed from the stash regardless
   * if the `unstash()` call successfully returns or throws an exception.
   */
  def unstash(): Unit = if (theStash.nonEmpty) try {
    enqueueFirst(theStash.head)
  } finally {
    theStash = theStash.tail
  }

  /**
   * Prepends all messages in the stash to the mailbox, and then clears the stash.
   *
   * Messages from the stash are enqueued to the mailbox until the capacity of the
   * mailbox (if any) has been reached. In case a bounded mailbox overflows, a
   * `MessageQueueAppendFailedException` is thrown.
   *
   * The stash is guaranteed to be empty after calling `unstashAll()`.
   */
  def unstashAll(): Unit = unstashAll(_ ⇒ true)

  /**
   * INTERNAL API.
   *
   * Prepends selected messages in the stash, applying `filterPredicate`,  to the
   * mailbox, and then clears the stash.
   *
   * Messages from the stash are enqueued to the mailbox until the capacity of the
   * mailbox (if any) has been reached. In case a bounded mailbox overflows, a
   * `MessageQueueAppendFailedException` is thrown.
   *
   * The stash is guaranteed to be empty after calling `unstashAll(Any => Boolean)`.
   *
   * @param filterPredicate only stashed messages selected by this predicate are
   *                        prepended to the mailbox.
   */
  private[akka] def unstashAll(filterPredicate: Any ⇒ Boolean): Unit = {
    try {
      val i = theStash.reverseIterator.filter(envelope ⇒ filterPredicate(envelope.message))
      while (i.hasNext) enqueueFirst(i.next())
    } finally {
      theStash = Vector.empty[Envelope]
    }
  }

  /**
   * INTERNAL API.
   *
   * Clears the stash and and returns all envelopes that have not been unstashed.
   */
  private[akka] def clearStash(): Vector[Envelope] = {
    val stashed = theStash
    theStash = Vector.empty[Envelope]
    stashed
  }

  /**
   * Enqueues `envelope` at the first position in the mailbox. If the message contained in
   * the envelope is a `Terminated` message, it will be ensured that it can be re-received
   * by the actor.
   */
  private def enqueueFirst(envelope: Envelope): Unit = {
    mailbox.enqueueFirst(self, envelope)
    envelope.message match {
      case Terminated(ref) ⇒ actorCell.terminatedQueuedFor(ref)
      case _ ⇒
    }
  }
}
