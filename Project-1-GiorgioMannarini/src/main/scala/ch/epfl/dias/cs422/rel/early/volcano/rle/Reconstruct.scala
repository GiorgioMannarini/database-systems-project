package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  NilRLEentry,
  NilTuple,
  RLEentry
}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Reconstruct[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  var result: IndexedSeq[RLEentry] = IndexedSeq[RLEentry]()
  var resIndex: Int = 0
  var resSize: Int = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    var leftRLE = left.next()
    var rightRLE = right.next()

    // CREDITS: https://www.youtube.com/watch?v=kbwk1Tw3OhE (more or less the same problem with calendar)
    while (leftRLE != NilRLEentry && rightRLE != NilRLEentry) {
      val leftIndex = leftRLE.get.startVID
      val rightIndex = rightRLE.get.startVID
      val leftEnd = leftRLE.get.endVID
      val rightEnd = rightRLE.get.endVID

      // If I have an overlap
      if (leftIndex <= rightEnd && rightIndex <= leftEnd) {
        val mergedValue = leftRLE.get.value ++ rightRLE.get.value
        val startIndex = leftIndex.max(rightIndex)
        val multiplier =
          leftEnd.min(rightEnd) + 1 - startIndex
        result :+= RLEentry(startIndex, multiplier, mergedValue)
      }
      if (leftEnd < rightEnd) {
        leftRLE = left.next()
      } else if (rightEnd < leftEnd) {
        rightRLE = right.next()
      } else {
        leftRLE = left.next()
        rightRLE = right.next()
      }
    }
    resSize = result.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (resIndex < resSize) {
      val nextRLE = result(resIndex)
      resIndex += 1
      Some(nextRLE)
    } else {
      NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
