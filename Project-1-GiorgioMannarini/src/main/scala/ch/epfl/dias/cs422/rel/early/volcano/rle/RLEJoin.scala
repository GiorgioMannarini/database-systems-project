package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private var finalResult: IndexedSeq[RLEentry] = IndexedSeq[RLEentry]() // This tuple will contain the final result
  private var resultIndex : Int = 0 //Index that will help me to return the result tuple by tuple
  private var resultSize : Int = 0 //Final result size
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    //Initializations
    left.open()
    right.open()
    //Building the two tables for the join
    var leftTable: IndexedSeq[RLEentry] = IndexedSeq[RLEentry]()
    var rightTable : IndexedSeq[RLEentry] = IndexedSeq[RLEentry]()
    var leftRLE = left.next()
    var rightRLE = right.next()

    if (leftRLE == NilRLEentry || rightRLE == NilRLEentry) return

    while (leftRLE != NilRLEentry) {
      leftTable :+= leftRLE.get
      leftRLE = left.next()
    }
    while (rightRLE != NilRLEentry) {
      rightTable :+= rightRLE.get
      rightRLE = right.next()
    }

    //Hash join. Credits: https://rosettacode.org/wiki/Hash_join#Scala
    // Revisited to work with RLE entries
    var index = -1
    val hash = rightTable.groupBy(left_row => getRightKeys.map(left_row.value)) withDefaultValue IndexedSeq()
    finalResult = leftTable.flatMap(left =>
      hash(getLeftKeys.map(left.value)).map(
        right => {
        index +=1
        RLEentry(index, right.length.max(left.length), left.value++right.value)}
        )
    )
    resultSize = finalResult.size
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (resultIndex < resultSize) {
      val nextRLE = finalResult(resultIndex)
      resultIndex += 1
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
