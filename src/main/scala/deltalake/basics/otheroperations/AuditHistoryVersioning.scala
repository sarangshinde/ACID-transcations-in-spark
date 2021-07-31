package deltalake.basics.otheroperations

import deltalake.basics.batch.CreateTable
import io.delta.tables.DeltaTable
import utilities.Constants._
import utilities.SparkFactory

class AuditHistoryVersioning() {

  val spark = SparkFactory.getSparkSession()

  def getLatestVersionsData(path: String): Unit = {

    println("Latest Version")
    spark.read.format(DELTA).load(path).show(false)

  }

  def getHistory(path: String): Unit = {

    println("Audit History")
    val deltaTable = DeltaTable.forPath(path)
    deltaTable.history().show(false)
  }

  def getSpecificVersion(path: String, version: String): Unit = {
    println(s"Version ${version}")
    spark.read.format(DELTA).option("versionAsOf", version)
      .load(path).show(false)

  }
}

object AuditHistoryVersioning {

  def main(args: Array[String]): Unit = {

    val auditHistoryVersioning = new AuditHistoryVersioning()

    val tablePath = DELTA_BASEPATH

    auditHistoryVersioning.getHistory(tablePath)
    auditHistoryVersioning.getLatestVersionsData(tablePath)
    auditHistoryVersioning.getSpecificVersion(tablePath, "0")
    auditHistoryVersioning.getSpecificVersion(tablePath, "1")
    auditHistoryVersioning.getSpecificVersion(tablePath, "2")
  }
}
