package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * A [[Store]] is an in-memory storage the data.
    *
    * Accessing the data is store-type specific and thus
    * you have to convert it to one of the subtypes.
    * See getRow for an example.
    */
  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )
  private var rowId : Int = 0 // Id of the next row to scan
  private val sc = scannable.asInstanceOf[RowStore] // To avoid to call the function
  private val totalRow = sc.getRowCount //Total number of rows in the table

  /**
    * NOTE: This function is commented because I don't need it
    *
    * Helper function (you do not have to use it or implement it)
    * It's purpose is to show how to convert the [[scannable]] to a
    * specific [[Store]].
    *
    * param rowId row number (startign from 0)
    * return the row as a Tuple
    */
 /* private def getRow(rowId: Int): Tuple = {
    scannable match {
      case rowStore: RowStore =>
        /**
          * For this project, it's safe to assume scannable will always
          * be a [[RowStore]].
          */
        rowStore.getRow(rowId)
    }
  } */


  /**
    * @inheritdoc
    */

  override def open(): Unit = this.rowId = 0

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    //Returning the next tuple of the table or NilTuple if the table is over
    if (rowId >= totalRow) {NilTuple} else {
      val newTuple= Some(sc.getRow(rowId))
      rowId += 1
      newTuple
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}
