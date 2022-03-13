package ch.epfl.dias.cs422

import ch.epfl.dias.cs422.helpers.SqlPrepare
import ch.epfl.dias.cs422.helpers.builder.Factories
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.sql.`type`.SqlTypeName

object Main {

  /**
    * If you want to use a custom input file for debugging add it in the list returned by
    * this function. The file will not be used during testing by the grader.
    *
    * Your file should be well formatted and with valid rows.
    */
  def getAdditionalSchema: Map[String, List[(String, SqlTypeName)]] = {
    Map(
      "/my/path/to/some/custom/tablefile.tbl" -> List(
        "l_orderkey" -> SqlTypeName.INTEGER,
        "l_partkey" -> SqlTypeName.INTEGER,
        "l_suppkey" -> SqlTypeName.INTEGER,
        "l_linenumber" -> SqlTypeName.INTEGER,
        "l_quantity" -> SqlTypeName.DECIMAL,
        "l_extendedprice" -> SqlTypeName.DECIMAL,
        "l_discount" -> SqlTypeName.DECIMAL,
        "l_tax" -> SqlTypeName.DECIMAL,
        "l_returnflag" -> SqlTypeName.VARCHAR,
        "l_linestatus" -> SqlTypeName.VARCHAR,
        "l_shipdate" -> SqlTypeName.DATE,
        "l_commitdate" -> SqlTypeName.DATE,
        "l_receiptdate" -> SqlTypeName.DATE,
        "l_shipinstruct" -> SqlTypeName.VARCHAR,
        "l_shipmode" -> SqlTypeName.VARCHAR,
        "l_comment" -> SqlTypeName.VARCHAR
      )
    )
  }

  def main(args: Array[String]): Unit = {
    val sql =
      """
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    tpch0_001_lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
      """

//    val sql = """
//select
//    l_returnflag,
//    l_linestatus,
//    sum(l_quantity) as sum_qty,
//    sum(l_extendedprice) as sum_base_price,
//    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//    avg(l_quantity) as avg_qty,
//    avg(l_extendedprice) as avg_price,
//    avg(l_discount) as avg_disc,
//    count(*) as count_order
//from
//    "/my/path/to/some/custom/tablefile.tbl"
//where
//    l_shipdate <= date '1998-12-01' - interval '90' day
//group by
//    l_returnflag,
//    l_linestatus
//order by
//    l_returnflag,
//    l_linestatus
//      """

    val prep = SqlPrepare(Factories.VOLCANO_INSTANCE, "rlestore")
    val rel = prep.prepare(sql)

    for (i <- 1 to 10) {
      println("Iteration " + i + " :")
      rel.asInstanceOf[Operator].foreach(println)
      //      // equivalent:
      //      rel.open()
      //      breakable {
      //        while (true) {
      //          val n = rel.next()
      //          if (n == Nil) break // It's not safe to call next again after it returns Nil
      //          println(n)
      //        }
      //      }
      //      rel.close()
    }
  }
}
