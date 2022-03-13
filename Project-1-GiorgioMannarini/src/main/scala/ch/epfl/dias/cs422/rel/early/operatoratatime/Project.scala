package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    // Unfortunately since I use evaluator (eval), I have to use it on the rows.
    // That's why I need to transpose two times
    val block = input.execute()
    if (block.isEmpty) {
      return block
    }
    var result : IndexedSeq[Tuple] = IndexedSeq[Tuple]()
    val trans = block.dropRight(1).transpose

    var index = 0

    while(index < trans.size){
      result :+= evaluator(trans(index))
      index += 1
    }
    result = result.transpose
    result :+= block.last
    result
  }
}
