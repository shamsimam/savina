package edu.rice.habanero.benchmarks.banking

import edu.rice.habanero.actors.{ScalaActor, ScalaActorState}
import edu.rice.habanero.benchmarks.banking.BankingConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BankingScalaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BankingScalaActorBenchmark)
  }

  private final class BankingScalaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BankingConfig.parseArgs(args)
    }

    def printArgInfo() {
      BankingConfig.printArgs()
    }

    def runIteration() {

      val master = new Teller(BankingConfig.A, BankingConfig.N)
      master.start()
      master.send(StartMessage.ONLY)

      ScalaActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  protected class Teller(numAccounts: Int, numBankings: Int) extends ScalaActor[AnyRef] {

    private val self = this
    private val accounts = Array.tabulate[Account](numAccounts)((i) => {
      new Account(i, BankingConfig.INITIAL_BALANCE)
    })
    private var numCompletedBankings = 0

    private val randomGen = new PseudoRandom(123456)


    protected override def onPostStart() {
      accounts.foreach(loopAccount => loopAccount.start())
    }

    override def process(theMsg: AnyRef) {
      theMsg match {

        case sm: BankingConfig.StartMessage =>

          var m = 0
          while (m < numBankings) {
            generateWork()
            m += 1
          }

        case sm: BankingConfig.ReplyMessage =>

          numCompletedBankings += 1
          if (numCompletedBankings == numBankings) {
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

  protected class Account(id: Int, var balance: Double) extends ScalaActor[AnyRef] {

    private val self = this

    override def process(theMsg: AnyRef) {
      theMsg match {
        case dm: DebitMessage =>

          balance += dm.amount
          val creditor = dm.sender.asInstanceOf[Account]
          creditor.send(ReplyMessage.ONLY)

        case cm: CreditMessage =>

          balance -= cm.amount
          val teller = cm.sender.asInstanceOf[ScalaActor[AnyRef]]

          val sender = self
          val destAccount = cm.recipient.asInstanceOf[Account]
          destAccount.send(new DebitMessage(sender, cm.amount))

          receive {
            case rm: ReplyMessage =>
              teller.send(ReplyMessage.ONLY)
          }

        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
