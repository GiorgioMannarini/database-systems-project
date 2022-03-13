package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }



  /**
    * @inheritdoc
    */
  override def open(): Unit = input.open()


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    var newTuple = input.next()

    // Logic to return a tuple only if it matches the condition:
    // basically if the actual tuple doesn't match the condition, checks the next one and so on
    while (newTuple != NilTuple){
      if (predicate(newTuple.get)) return newTuple
      newTuple = input.next()
    }
    NilTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
