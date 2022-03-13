package ch.epfl.dias.cs422

import ch.epfl.dias.cs422.helpers.SqlPrepare
import ch.epfl.dias.cs422.helpers.builder.Factories
import org.junit.jupiter.api.{DynamicNode, TestFactory}

import java.io.IOException
import java.util

class QueryTest extends ch.epfl.dias.cs422.util.QueryTest {

  @TestFactory
  @throws[IOException]
  override protected[cs422] def tests: util.List[DynamicNode] = {
    runTests(
      List(
        "volcano (row store)" -> SqlPrepare(
          Factories.VOLCANO_INSTANCE,
          "rowstore"
        )
      )
    )
  }
}
