package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * NOTE: in column-at-a-time execution with selection vectors, the
  * filter does not prune the tuples, only marks the corresponding
  * entries in the selection vector as false.
  * Removing tuples will be penalized.
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val mappredicate: IndexedSeq[HomogeneousColumn] => Array[Boolean] = {
    val evaluator = map(condition, input.getRowType, isFilterCondition = true)
    (t: IndexedSeq[HomogeneousColumn]) => unwrap[Boolean](evaluator(t))
  }

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[HomogeneousColumn] = {

    // Now I can filter directly on columns
    val block = input.execute()
    if (block.isEmpty) {
      return IndexedSeq[HomogeneousColumn]()
    }
    val inputVector = block.last.toIndexedSeq
    val data = block.dropRight(1)
    val resultVector = mappredicate(data).zip(inputVector).map(r => r._1 && r._2.asInstanceOf[Boolean])

    data :+ toHomogeneousColumn(resultVector)



  }
}
