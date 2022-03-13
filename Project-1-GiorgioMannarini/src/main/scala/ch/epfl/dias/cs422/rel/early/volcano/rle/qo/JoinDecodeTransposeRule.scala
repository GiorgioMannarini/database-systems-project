package ch.epfl.dias.cs422.rel.early.volcano.rle.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalDecode
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.logical.LogicalJoin

/**
  * A RelRule (optimization rule) that finds a join over two Decodes
  * and pulls the decoding above the join.
  *
  * To use this rule: JoinDecodeTransposeRuleConfig.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class JoinDecodeTransposeRule protected (
    config: RelRule.Config
) extends RelRule[RelRule.Config](config) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)
    val leftDecode: LogicalDecode = call.rel(1)
    val rightDecode: LogicalDecode = call.rel(2)

    /* use the RelNode.copy to create new nodes from existing ones
     * for example, to create a new join:
     *   join.copy(join.getTraitSet, <list of new inputs>)
     */

    call.transformTo(
      leftDecode.copy(
        join.copy(
          join.getTraitSet,
          java.util.List.of(leftDecode.getInput, rightDecode.getInput)
        )
      )
    )
  }
}

object JoinDecodeTransposeRule {

  /**
    * Configuration for a [[JoinDecodeTransposeRule]]
    */
  val INSTANCE = new JoinDecodeTransposeRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
    // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalJoin]
        b.operand(classOf[LogicalJoin])
          // that has inputs:
          .inputs(
            b1 =>
              // A node that is a LogicalDecode
              b1.operand(classOf[LogicalDecode])
                // of any inputs
                .anyInputs(),
            b2 =>
              // A node that is a LogicalDecode
              b2.operand(classOf[LogicalDecode])
                // of any inputs
                .anyInputs()
          )
      )
  )

}
