package edu.rice.habanero.benchmarks.banking

import edu.rice.habanero.actors.HabaneroActor
import edu.rice.habanero.benchmarks.banking.BankingConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}
import edu.rice.hj.Module0._
import edu.rice.hj.api.{HjDataDrivenFuture, HjRunnable, HjSuspendable}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BankingHabaneroPauseResumeActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BankingHabaneroPauseResumeActorBenchmark)
  }

  private final class BankingHabaneroPauseResumeActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      parseArgs(args)
    }

    def printArgInfo() {
      printArgs()
    }

    def runIteration() {

      var master: Teller = null
      finish(new HjSuspendable {
        override def run() = {
          master = new Teller(A, N)
          master.start()
          master.send(StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  protected class Teller(numAccounts: Int, numTransactions: Int) extends HabaneroActor[AnyRef] {

    private val self = this
    private val accounts = Array.tabulate[Account](numAccounts)((i) => {
      new Account(i, INITIAL_BALANCE)
    })
    private var numCompletedTransactions = 0

    private val randomGen = new PseudoRandom(123456)


    protected override def onPostStart() {
      accounts.foreach(loopAccount => loopAccount.start())
    }

    override def process(theMsg: AnyRef) {
      theMsg match {

        case sm: StartMessage =>

          var m = 0
          while (m < numTransactions) {
            generateWork()
            m += 1
          }

        case sm: ReplyMessage =>

          numCompletedTransactions += 1
          if (numCompletedTransactions == numTransactions) {
            accounts.foreach(loopAccount => loopAccount.send(StopMessage.ONLY))
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    def generateWork(): Unit = {
      // src is lower than dest id to ensure there is never a deadlock
      val srcAccountId = randomGen.nextInt((accounts.length / 10) * 8)
      var loopId = randomGen.nextInt(accounts.length - srcAccountId)
      if (loopId == 0) {
        loopId += 1
      }
      val destAccountId = srcAccountId + loopId

      val srcAccount = accounts(srcAccountId)
      val destAccount = accounts(destAccountId)
      val amount = Math.abs(randomGen.nextDouble()) * 1000

      val sender = self
      val cm = new CreditMessage(sender, amount, destAccount)
      srcAccount.send(cm)
    }
  }

  protected class Account(val id: Int, var balance: Double) extends HabaneroActor[AnyRef] {

    private val self = this

    override def process(theMsg: AnyRef) {
      theMsg match {
        case dm: DebitMessage =>

          balance += dm.amount
          val creditor = dm.sender.asInstanceOf[HjDataDrivenFuture[ReplyMessage]]
          creditor.put(ReplyMessage.ONLY)

        case cm: CreditMessage =>

          balance -= cm.amount
          val teller = cm.sender.asInstanceOf[HabaneroActor[AnyRef]]

          val sender = newDataDrivenFuture[ReplyMessage]()
          val destAccount = cm.recipient.asInstanceOf[Account]
          destAccount.send(new DebitMessage(sender, cm.amount))
          pause()
          asyncNbAwait(sender, new HjRunnable {
            override def run() = {
              teller.send(ReplyMessage.ONLY)
              resume()
            }
          })

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
