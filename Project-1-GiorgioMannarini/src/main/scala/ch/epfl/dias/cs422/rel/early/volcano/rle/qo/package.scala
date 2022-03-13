package ch.epfl.dias.cs422.rel.early.volcano.rle

import org.apache.calcite.rel.rules.CoreRules
import ch.epfl.dias.cs422.helpers.qo.rules.RemoveEmptyReconstructLeft
import ch.epfl.dias.cs422.helpers.qo.rules.RemoveEmptyReconstructRight

package object qo {
  /**
   * For the test-cases that use the extended list of
   * optimizations, this list of rules is appended
   * to the rules of the last logical optimization pass.
   *
   * Optionally consider adding or removing lists to
   * accelerate your queries.
   */
  val selectedOptimizations =
    List(
      CoreRules.PROJECT_REMOVE,
      RemoveEmptyReconstructLeft.INSTANCE,
      RemoveEmptyReconstructRight.INSTANCE
    )
}
