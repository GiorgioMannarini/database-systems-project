package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode



/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

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
  def execute(): IndexedSeq[Column] = {
    val block = input.execute()
    if (block.isEmpty){
      return block
    }
    val trans = block.dropRight(1).transpose
    val inputVector = block.last.asInstanceOf[IndexedSeq[Boolean]]
    var index = 0
    var outputVector : IndexedSeq[Any] = IndexedSeq[Any]()
    while(index < trans.size){
      // I need to check the condition in end with the old vector
      outputVector :+= predicate(trans(index)) & inputVector(index)
      index += 1
    }
    val result = block.dropRight(1) :+ outputVector
    result
  }
}
