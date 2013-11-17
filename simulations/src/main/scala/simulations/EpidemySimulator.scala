package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8

    // to complete: additional parameters of simulation
    val prevalenceRate = 0.01
    val transmissibilityRate = 0.4
    val dieRate = 0.25
    val incubationTime = 6
    val dieTime = 14
    val immuneTime = 16
    val healTime = 18
    val waitingTimeInterval = (1, 5)
  }

  import SimConfig._

  val persons: List[Person] = buildPersons // to complete: construct list of persons


  def buildPersons: List[Person] = {
    var p: List[Person] = Nil
    for (id <- 0 until population) {
      val person = new Person(id)
      if (id < population * prevalenceRate) {
        person.infected = true
        person.lastEventTime = 0
      }
      p = person :: p
    }
    p
  }

  class Person(val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    //
    // to complete with simulation logic
    //
    var waitCount = 0
    var lastEventTime = 0
  }

  afterDelay(1) { step }

  def step() {
    persons.filter(!_.dead) foreach {
      p =>
        move(p)
        infectRule(p)
        sickRule(p)
        dieRule(p)
        immuneRule(p)
        healthyRule(p)
    }

    afterDelay(1) { step }
  }

  def personsInField(position: (Int, Int)): List[Person] = {
    persons.filter(p => p.row == position._1 && p.col == position._2)
  }

  // Rule 1, 2
  def move(p: Person) {
    def oneOf(ints: List[Int]): Int = {
      ints match {
        case Nil => -1
        case _ => {
          val chosen = randomBelow(ints.size)
          ints(chosen)
        }
      }
    }
    val UP = 0
    val RIGHT = 1
    val DOWN = 2
    val LEFT = 3
    def choseDirection(directions: List[Int]) {
      def moveToPosition(position: (Int, Int), dir: Int) {
        val neighbours = personsInField(position)
        if (!neighbours.exists(p => p.sick || p.dead)) {
          p.row = position._1
          p.col = position._2
        } else
          choseDirection(directions.drop(dir))
      }

      val newRoom = oneOf(directions)
      newRoom match {
        case UP => {
          val newPosition = (if (p.row > 0) p.row - 1 else roomRows - 1, p.col)
          moveToPosition(newPosition, UP)
        }
        case RIGHT => {
          val newPosition = (p.row, (p.col + 1) % roomColumns)
          moveToPosition(newPosition, RIGHT)
        }
        case DOWN => {
          val newPosition = ((p.row + 1) % roomRows, p.col)
          moveToPosition(newPosition, DOWN)
        }
        case LEFT => {
          val newPosition = (p.row, if (p.col > 0) p.col - 1 else roomColumns - 1)
          moveToPosition(newPosition, LEFT)
        }
        // last mean used if all neighbour cells are visible infected
        case _ => {
          p.row = (p.row + 1) % roomRows
        }
      }
    }
    // Method logic
    if (p.waitCount > 1) p.waitCount = p.waitCount - 1
    else {
      if (p.waitCount < 1) {
        p.waitCount = randomBelow(waitingTimeInterval._2) + 1
      }
      //move
      if (p.waitCount == 1) {
        val directions = UP :: RIGHT :: DOWN :: LEFT :: Nil
        choseDirection(directions)
        p.waitCount = 0
      }
    }
  }

  //Rule 3
  def infectRule(p: Person) {
    if (!p.infected && !p.immune) {
      val existDanger: Boolean = personsInField((p.row, p.col)).exists( p => p.infected || p.immune)
      if (existDanger && randomBelow(100) < transmissibilityRate * 100) {
        p.infected = true
        p.lastEventTime = currentTime
      }
    }
  }

  //Rule 4, 5
  def sickRule(p: Person) {
    if (p.infected && currentTime - p.lastEventTime > incubationTime) {
      p.sick = true
      p.lastEventTime = currentTime
    }
  }

  //Rule 6
  def dieRule(p: Person) {
    if (p.sick && currentTime - p.lastEventTime > dieTime - incubationTime) {
      if (randomBelow(100) < dieRate * 100) {
        p.dead = true
        p.lastEventTime = currentTime
      }
    }
  }

  //Rule 7
  def immuneRule(p: Person) {
    if (p.sick && currentTime - p.lastEventTime > immuneTime - incubationTime) {
      p.immune = true
      p.sick = false
      p.lastEventTime = currentTime
    }
  }

  //Rule 8
  def healthyRule(p: Person) {
    if (p.immune && currentTime - p.lastEventTime > healTime - immuneTime) {
      p.immune = false
      p.lastEventTime = currentTime
    }
  }
}
