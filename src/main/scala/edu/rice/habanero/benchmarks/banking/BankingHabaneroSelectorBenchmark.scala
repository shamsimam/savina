package edu.rice.habanero.benchmarks.banking

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.banking.BankingConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BankingHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BankingHabaneroSelectorBenchmark)
  }

  private final class BankingHabaneroSelectorBenchmark extends Benchmark {
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
          master.send(0, StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  protected class Teller(numAccounts: Int, numTransactions: Int) extends HabaneroSelector[AnyRef](1) {

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
            accounts.foreach(loopAccount => loopAccount.send(0, StopMessage.ONLY))
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
      srcAccount.send(Account.CREDIT_BOX, cm)
    }
  }

  object Account {
    val DEBIT_BOX = 0
    val CREDIT_BOX = 1
    val REPLY_BOX = 2
  }

  protected class Account(val id: Int, var balance: Double) extends HabaneroSelector[AnyRef](3) {

    private val self = this
    private var teller: HabaneroSelector[AnyRef] = null

    override def process(theMsg: AnyRef) {
      theMsg match {
        case dm: DebitMessage =>

          balance += dm.amount
          val creditor = dm.sender.asInstanceOf[HabaneroSelector[AnyRef]]
          creditor.send(Account.REPLY_BOX, ReplyMessage.ONLY)

        case cm: CreditMessage =>

          balance -= cm.amount
          teller = cm.sender.asInstanceOf[HabaneroSelector[AnyRef]]

          val sender = self
          val destAccount = cm.recipient.asInstanceOf[Account]
          destAccount.send(Account.DEBIT_BOX, new DebitMessage(sender, cm.amount))
          disable(Account.DEBIT_BOX)
          disable(Account.CREDIT_BOX)

        case rm: ReplyMessage =>

          enable(Account.DEBIT_BOX)
          enable(Account.CREDIT_BOX)
          teller.send(0, ReplyMessage.ONLY)

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
