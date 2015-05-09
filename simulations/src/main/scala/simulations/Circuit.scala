package simulations


class Wire {
  private var sigVal = false
  private var actions: List[Simulator#Action] = List()

  def getSignal: Boolean = sigVal

  def setSignal(s: Boolean) {
    if (s != sigVal) {
      sigVal = s
      actions.foreach(action => action())
    }
  }

  def addAction(a: Simulator#Action) {
    actions = a :: actions
    a()
  }
}

abstract class CircuitSimulator extends Simulator {

  val InverterDelay: Int
  val AndGateDelay: Int
  val OrGateDelay: Int

  def probe(name: String, wire: Wire) {
    wire addAction {
      () => afterDelay(0) {
        println(
          "  " + currentTime + ": " + name + " -> " + wire.getSignal)
      }
    }
  }

  def inverter(input: Wire, output: Wire) {
    def invertAction() {
      val inputSig = input.getSignal
      afterDelay(InverterDelay) {
        output.setSignal(!inputSig)
      }
    }
    input addAction invertAction
  }

  def andGate(a1: Wire, a2: Wire, output: Wire) {
    def andAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(AndGateDelay) {
        output.setSignal(a1Sig & a2Sig)
      }
    }
    a1 addAction andAction
    a2 addAction andAction
  }

  //
  // to complete with orGates and demux...
  //

  def orGate(a1: Wire, a2: Wire, output: Wire) {
    def orAction() {
      val a1Sig = a1.getSignal
      val a2Sig = a2.getSignal
      afterDelay(OrGateDelay) {
        output.setSignal(a1Sig | a2Sig)
      }
    }
    a1 addAction orAction
    a2 addAction orAction
  }

  def orGate2(a1: Wire, a2: Wire, output: Wire) {
    val notA1 = new Wire
    inverter(a1, notA1)
    val notA2 = new Wire
    inverter(a2, notA2)
    val notOut = new Wire
    andGate(notA1, notA2, notOut)
    inverter(notOut, output)
  }

  def demux(in: Wire, c: List[Wire], out: List[Wire]) {
    c match {
      case Nil =>
      case head :: rest => {
        val out1 = out.head
        val out2 = out.tail.head
        orGate(in, head, out1)
        inverter(out1, out2)
        val nextOut = out.tail.tail
        demux(out1, rest, nextOut)
        demux(out2, rest, nextOut)
      }
    }
  }

}

object Circuit extends CircuitSimulator {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  def andGateExample() {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(s = false)
    in2.setSignal(s = false)
    run()

    in1.setSignal(s = true)
    run()

    in2.setSignal(s = true)
    run()
  }

  //
  // to complete with orGateExample and demuxExample...
  //
  def orGateExample() {
    println()
    println("----- Or Gate Example")
    val in1, in2, out = new Wire
    orGate(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(s = false)
    in2.setSignal(s = false)
    run()

    in1.setSignal(s = false)
    run()

    in2.setSignal(s = true)
    run()
  }

  def orGate2Example() {
    println()
    println("----- Or Gate2 Example")
    val in1, in2, out = new Wire
    orGate2(in1, in2, out)
    probe("in1", in1)
    probe("in2", in2)
    probe("out", out)
    in1.setSignal(s = false)
    in2.setSignal(s = false)
    run()

    in1.setSignal(s = false)
    run()

    in2.setSignal(s = true)
    run()
  }
}

object CircuitMain extends App {
  // You can write tests either here, or better in the test class CircuitSuite.
  Circuit.andGateExample()

  Circuit.orGateExample()

  Circuit.orGate2Example()
}
