package ch.epfl.dias.cs422

import ch.epfl.dias.cs422.QueryTest.defaultOptimizationRules
import ch.epfl.dias.cs422.helpers.SqlPrepare
import ch.epfl.dias.cs422.helpers.builder.Factories
import ch.epfl.dias.cs422.helpers.qo.rules.{ProjectDecodeTransposeRule, ProjectReconstructTransposeRule}
import ch.epfl.dias.cs422.rel.early.volcano.rle.qo
import ch.epfl.dias.cs422.rel.early.volcano.rle.qo.{AggregateDecodeTransposeRule, FilterDecodeTransposeRule, FilterReconstructTransposeRule, JoinDecodeTransposeRule}
import org.apache.calcite.rel.rules.CoreRules
import org.junit.jupiter.api.{DynamicNode, TestFactory}

import java.io.IOException
import java.util

object QueryTest{
  val defaultOptimizationRules = List(
    CoreRules.PROJECT_FILTER_TRANSPOSE,
    CoreRules.PROJECT_JOIN_TRANSPOSE,
    new ProjectDecodeTransposeRule,
    FilterDecodeTransposeRule.INSTANCE,
    AggregateDecodeTransposeRule.INSTANCE,
    JoinDecodeTransposeRule.INSTANCE,
    FilterReconstructTransposeRule.INSTANCE,
    new ProjectReconstructTransposeRule,
  )
}

class QueryTest extends ch.epfl.dias.cs422.util.QueryTest {

  @TestFactory
  @throws[IOException]
  override protected[cs422] def tests: util.List[DynamicNode] = {
    runTests(
      List(
        "volcano (RLE store)" -> SqlPrepare(
          Factories.VOLCANO_INSTANCE,
          "rlestore"
        ),
        "volcano RLE (RLE store)" -> SqlPrepare(
          Factories.VOLCANO_RLE_INSTANCE,
          "rlestore"
        ),
        "volcano RLE with rules (RLE store)" -> SqlPrepare(
          Factories.VOLCANO_RLE_INSTANCE,
          "rlestore",
          defaultOptimizationRules ++ qo.selectedOptimizations
        ),
        "operator-at-a-time (column store)" -> SqlPrepare(
          Factories.OPERATOR_AT_A_TIME_INSTANCE,
          "columnstore"
        ),
        "column-at-a-time (column store)" -> SqlPrepare(
          Factories.COLUMN_AT_A_TIME_INSTANCE,
          "columnstore",
          defaultOptimizationRules ++ qo.selectedOptimizations
        )
      )
    )
  }
}
