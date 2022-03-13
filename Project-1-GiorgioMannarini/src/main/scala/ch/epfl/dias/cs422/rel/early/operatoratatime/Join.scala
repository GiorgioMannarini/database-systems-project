package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    var leftTable = left.execute()
    var rightTable = right.execute()


    // If one of the two tables is empty, since it's an inner join, I have to return an empty table
    if(leftTable.isEmpty || rightTable.isEmpty){
      return IndexedSeq()
    }

    var result : IndexedSeq[Column] = IndexedSeq[Column]()

    // Separating the true columns from their bit vectors
    val leftVector = leftTable.last.asInstanceOf[IndexedSeq[Boolean]]
    val rightVector = rightTable.last.asInstanceOf[IndexedSeq[Boolean]]
    leftTable = leftTable.dropRight(1)
    rightTable = rightTable.dropRight(1)


    // Materializing only JOIN columns as rows
    val leftRows = getLeftKeys.map(key => leftTable(key)).transpose
    val rightRows = getRightKeys.map(key => rightTable(key)).transpose

    // I need the indexes because I am joining only on key columns but then I need to join also the others
    val hash = rightRows.zipWithIndex.groupBy(_._1) withDefaultValue IndexedSeq()

    // Now I have a list of all the indices in pairs. Note that here I also remove the rows that I should not
    // Consider because of the input bit vectors
    val finalIndices = leftRows.zipWithIndex.filter(lr => leftVector(lr._2)).
      flatMap(lr => hash(lr._1).filter(rr => rightVector(rr._2)).map(rr => {
      List(lr._2, rr._2)
    }))

    // Building the columns of the result, starting from the ones of the left table, then moving
    // To the right table
    var leftResult = leftTable.map(_ => IndexedSeq[RelOperator.Elem]())
    var rightResult = rightTable.map(_ => IndexedSeq[RelOperator.Elem]())
    finalIndices.foreach(indexPair => {
      val leftIndex = indexPair.head
      val rightIndex = indexPair.last
      var leftColIndex = 0
      var rightColIndex = 0
      leftResult = leftTable.map(leftCol => {
        leftColIndex +=1
        leftResult(leftColIndex -1) :+ leftCol(leftIndex)
      })
      rightResult = rightTable.map(rightCol => {
        rightColIndex +=1
        rightResult(rightColIndex -1) :+ rightCol(rightIndex)
      })
    })


    result = leftResult ++ rightResult
    result :+= IndexedSeq.fill(result(0).size)(true)
    result
  }
}
