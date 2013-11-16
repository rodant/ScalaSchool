package quickcheck


import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  property("nin2") = forAll { (i1: Int, i2: Int) =>
    if (i1 <= i2) findMin(insert(i1, insert(i2, empty))) == i1
    else findMin(insert(i1, insert(i2, empty))) == i2
  }

  property("empty") = forAll { a: Int =>
    deleteMin(insert(a, empty)) == empty
  }

  property("increasing sequence") = forAll { h : H =>
    def isLowBound(last: Int, h: H): Boolean = {
      if (isEmpty(h)) true
      else {
        val min = findMin(h)
        if (last > min) false
        else isLowBound(min, deleteMin(h))
      }
    }

    def isIncreasing(h: H): Boolean = isLowBound(Int.MinValue, h)

    try {
      isIncreasing(h)
    } catch {
      case e: Throwable => false
    }
  }

  property("min of melding") = forAll { (h1 : H, h2: H) =>
    val min1 = findMin(h1)
    val min2 = findMin(h2)
    val minMelding = findMin(meld(h1, h2))

    minMelding == min1 || minMelding == min2
  }

  property("gen1") = forAll { h: H =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }

  def toIncreasingList(h: H): List[Int] = {
    def preppedToList(h: H, l: List[Int]): List[Int] = {
      if (isEmpty(h)) l
      else preppedToList(deleteMin(h), findMin(h) :: l)
    }

    preppedToList(h, Nil)
  }

  property("melding contains all") = forAll { (h1: H, h2: H) =>
    val list1 = toIncreasingList(h1)
    val list2 = toIncreasingList(h2)
    val listMelding = toIncreasingList(meld(h1, h2))

    listMelding.size == list1.size + list2.size &&
      (list1 ::: list2).forall(listMelding.contains(_))
  }

  lazy val genHeap: Gen[H] = for {
    i <- arbitrary[Int]
    h <- oneOf(value(empty), genHeap)
  } yield insert(i, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
