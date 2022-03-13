package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    var result: IndexedSeq[HomogeneousColumn] = IndexedSeq[HomogeneousColumn]()
    if (scannable.getRowCount == 0){
      return result //CASE OF AN EMPTY TABLE
    }
    var index = 0
    while(index < getRowType.getFieldCount){
      result :+= scannable.getColumn(index)
      index += 1
    }
    result :+= IndexedSeq.fill(scannable.getRowCount.toInt)(true)
    result
  }
}
