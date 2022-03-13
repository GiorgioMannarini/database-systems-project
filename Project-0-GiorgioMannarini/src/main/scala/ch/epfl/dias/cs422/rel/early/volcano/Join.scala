package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * @inheritdoc
    */


  var finalResult: IndexedSeq[Tuple] = IndexedSeq[Tuple]() // This tuple will contain the final result
  var resultIndex : Int = 0 //Index that will help me to return the result tuple by tuple
  var resultSize : Int = 0 //Final result size

  override def open(): Unit = {

    //Initializations
    left.open()
    right.open()

    //Building the two tables for the join
    var leftTable: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
    var rightTable : IndexedSeq[Tuple] = IndexedSeq[Tuple]()

    var leftRow = left.next()
    while (leftRow != NilTuple) {
      leftTable :+= leftRow.get
      leftRow = left.next()
    }

    var rightRow = right.next()
    while (rightRow != NilTuple) {
      rightTable :+= rightRow.get
      rightRow = right.next()
    }


    //Hash join. Credits: https://rosettacode.org/wiki/Hash_join#Scala
    val hash = rightTable.groupBy(left_row => getRightKeys.map(left_row)) withDefaultValue IndexedSeq()
    finalResult = leftTable.flatMap(cols => hash(getLeftKeys.map(cols)).map(cols ++ _))

    resultSize = finalResult.size
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (resultIndex >= resultSize){
      return NilTuple
    }
    val nextTuple = finalResult(resultIndex)
    resultIndex +=1
    Some(nextTuple)
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}











