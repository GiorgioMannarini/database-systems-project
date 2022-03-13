package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]


  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {


    if (scannable.getRowCount == 0){
      return IndexedSeq() //CASE OF AN EMPTY TABLE
    }
    var result: IndexedSeq[Column] = IndexedSeq[Column]()
    var index = 0
    while(index < getRowType.getFieldCount){
      result :+= scannable.getColumn(index).toIndexedSeq
      index += 1
    }
    result :+= IndexedSeq.fill(scannable.getRowCount.toInt)(true)
    result
    }

}
