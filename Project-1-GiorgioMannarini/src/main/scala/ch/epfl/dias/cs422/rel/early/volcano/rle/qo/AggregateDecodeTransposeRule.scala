package ch.epfl.dias.cs422.rel.early.volcano.rle.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalDecode
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.logical.LogicalAggregate

/**
  * RelRule (optimization rule) that finds an aggregate above a decode
  * and pushes it bellow it.
  *
  * To use this rule: AggregateDecodeTransposeRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class AggregateDecodeTransposeRule protected (config: RelRule.Config)
    extends RelRule(
      config
    ) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: LogicalAggregate = call.rel(0)
    val decode: LogicalDecode = call.rel(1)

    call.transformTo(
      decode.copy(
        agg.copy(
          agg.getTraitSet,
          java.util.List.of(decode.getInput)
        )
      )
    )
  }
}

object AggregateDecodeTransposeRule {

  /**
    * Instance for a [[AggregateDecodeTransposeRule]]
    */
  val INSTANCE = new AggregateDecodeTransposeRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
    // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalAggregate]
        b.operand(classOf[LogicalAggregate])
          // that has inputs:
          .oneInput(b1 =>
            // A node that is a LogicalDecode
            b1.operand(classOf[LogicalDecode])
              // of any inputs
              .anyInputs()
          )
      )
  )
}
