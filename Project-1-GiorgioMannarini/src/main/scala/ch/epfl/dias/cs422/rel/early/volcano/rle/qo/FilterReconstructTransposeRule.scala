package ch.epfl.dias.cs422.rel.early.volcano.rle.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalReconstruct
import org.apache.calcite.plan.{RelOptRuleCall, RelOptUtil, RelRule}
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.RexUtil

/**
  * RelRule (optimization rule) that finds a filter above a reconstruct
  * and if the filter references only fields in one of the two inputs
  * participating in the reconstruction, then it pushes the filter
  * towards that input.
  *
  * To use this rule: FilterReconstructTransposeRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class FilterReconstructTransposeRule protected (config: RelRule.Config)
    extends RelRule[RelRule.Config](config) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: LogicalFilter = call.rel(0)
    val recons: LogicalReconstruct = call.rel(1)

    val inputs =
      RelOptUtil.InputFinder.analyze(filter.getCondition).build()

    if (inputs.isEmpty) return

    val first = inputs.nextSetBit(0) // 0-based
    val last = inputs.nth(inputs.cardinality() - 1) // 0-based
    assert(first <= last)

    if (first >= recons.getLeft.getRowType.getFieldCount) {
      /*
       * Filter references right-hand-side child only,
       * so push the filter to that side
       */

      // Find the condition of the new filter (shift inputs of currrent condition)
      val newCondition = RexUtil.shift(
        filter.getCondition,
        -recons.getLeft.getRowType.getFieldCount
      )

      call.transformTo(
        recons.copy(
          left = recons.getLeft,
          LogicalFilter.create(
            recons.getRight,
            newCondition
          )
        )
      )
    } else if (last < recons.getLeft.getRowType.getFieldCount) {
      /*
       * Filter references left-hand-side child only,
       * so push the filter to that side
       */

      // Find the condition of the new filter (equivalent to current condition)
      val newCondition = filter.getCondition

      call.transformTo(
        recons.copy(
          left = LogicalFilter.create(
            recons.getLeft,
            newCondition
          ),
          recons.getRight
        )
      )
    }
  }
  /* Else, the filter references both sides,
   * so we can't push it to just one side.
   *
   * Optionally, you can try to implement the rest of
   * the rule to check if the filter can be broken down
   * into two filters with each of them referencing only
   * one input. If it can, then you can push one filter
   * on each side.
   */
}

object FilterReconstructTransposeRule {

  /**
    * Configuration for a [[FilterReconstructTransposeRule]]
    */
  val INSTANCE = new FilterReconstructTransposeRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
    // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalFilter]
        b.operand(classOf[LogicalFilter])
          // that has inputs:
          .oneInput(b1 =>
            // A node that is a LogicalReconstruct
            b1.operand(classOf[LogicalReconstruct])
              // of any inputs
              .anyInputs()
          )
      )
  )

}
