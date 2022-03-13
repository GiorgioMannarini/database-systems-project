package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    // Initializations
    var leftTable = left.execute()
    var rightTable = right.execute()

    // Returning if empty table
    if(leftTable.isEmpty || rightTable.isEmpty){
      return IndexedSeq()
    }

    var result : IndexedSeq[HomogeneousColumn] = IndexedSeq[HomogeneousColumn]()

    // Separating the true columns from their bit vectors
    val leftVector = leftTable.last.toIndexedSeq.asInstanceOf[IndexedSeq[Boolean]]
    val rightVector = rightTable.last.toIndexedSeq.asInstanceOf[IndexedSeq[Boolean]]
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
    leftTable.foreach(leftCol => {
      var finalLeftCol : IndexedSeq[Any] = IndexedSeq[Any]()
      val leftIndexSeq = leftCol.toIndexedSeq
      finalIndices.foreach(indexList => {
        val leftIndex = indexList.head
        finalLeftCol :+= leftIndexSeq(leftIndex)
      })
      result :+= finalLeftCol
    })

    rightTable.foreach(rightCol => {
      var finalRightCol : IndexedSeq[Any] = IndexedSeq[Any]()
      val rightIndexSeq = rightCol.toIndexedSeq
      finalIndices.foreach(indexList => {
        val rightIndex = indexList(1)
        finalRightCol :+= rightIndexSeq(rightIndex)
      })
      result :+= finalRightCol
    })

    // Finally, I create and append the new output vector (with every bit set to true)
    result :+= IndexedSeq.fill(result(0).size)(true)
    result
  }
}
