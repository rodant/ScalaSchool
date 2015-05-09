package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalacheck.{Gen, Arbitrary, Properties, Prop}
import Arbitrary._
import Gen._
import Prop._
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite with Checkers {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5
  
  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "and 3")
  }

  //
  // to complete with tests for orGate, demux, ...
  //

  lazy val wireGen: Gen[Wire] = for {
    s <- arbitrary[Boolean]
  } yield {
    val w = new Wire()
    w.setSignal(s)
    w
  }

  implicit lazy val arbWire = Arbitrary(wireGen)

  test("demux properties") {
    val properties = new Properties("demux") {
      def genOut(c: List[Wire]): List[Wire] = {
        if (c.isEmpty) List(new Wire)
        /*else {
          c match {
            case Nil => c
            case h :: t if !t.isEmpty => new Wire() :: new Wire() :: genOut(t)
            case h :: t => new Wire() :: new Wire() :: Nil
          }
        }*/
        else {
          val out = (for (w1 <- c; w2 <- c) yield w1 :: w2 :: Nil).flatMap(l => l)
          out.foreach(_.setSignal(false))
          out
        }
      }

      def intPower(b: Int, e: Int): Int = e match {
        case 0 => 1
        case exp if exp > 0 => b * intPower(b, exp - 1)
      }

      property("demux generates 2^n outputs") = forAll { c: List[Wire] =>
        val in = new Wire()
        val out = genOut(c)
        demux(in, c, out)
        (out.size == intPower(2, c.size)) :| "out.size == 2^(c.size)"
      }
    }

    //check(properties)
  }
}
