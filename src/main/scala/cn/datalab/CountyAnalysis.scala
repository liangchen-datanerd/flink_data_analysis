import cn.datalab.entity.countyInfo
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem.WriteMode

object CountyAnalysis {
  def main(args: Array[String]): Unit = {


    getMaxAreaCountyInState("Alaska",getClass.getResource("counties.csv").getPath)

    getStateInfo(getClass.getResource("counties.csv").getPath,getClass.getResource("/").getPath)

    getStateTop10(getClass.getResource("/").getPath)


  }

  /**
   * calculate which county have largest area for a state
   * @param StateName
   * @param inputPath
   */
  def getMaxAreaCountyInState(StateName: String, inputPath: String) = {

    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // data input
    val input = env.readCsvFile[countyInfo](inputPath, ignoreFirstLine = true)

    val county = input.filter(_.state == StateName) // filter state
      .map(x => (x.area, x.county, x.state)) //keep useful colunms
      .maxBy(0)
      .map(x => x._2)


    println(s"max area for $StateName is ")
    county.print()
  }

  /**
   * calculate  total population total area and population density for every state
   * @param inputPath
   * @param outPut
   */
  def getStateInfo(inputPath: String, outPut: String) = {
    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // data input
    val input = env.readCsvFile[countyInfo](inputPath, ignoreFirstLine = true)

    val stateInfo = input.map(x => (x.population, x.area, x.state)) //keep useful colunms
      .groupBy(2) // aggregate by state
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3))
      .map((x) => (x._1, x._2, (x._1.asInstanceOf[Float] / x._2.asInstanceOf[Float]).formatted("%.2f"), x._3))
      .setParallelism(1)

    stateInfo.writeAsCsv(outPut+ "stateInfo.csv", writeMode=WriteMode.OVERWRITE)
    stateInfo.print()
  }

  def getStateTop10(relativePath: String) = {

    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // data input
    val input = env.readCsvFile[(Int,Int,Float,String)](relativePath+ "stateInfo.csv", ignoreFirstLine = true)


    // calculate top 10 population
    val populationTop10 = input.map(x => (x._1, x._4))
      .sortPartition(0, Order.DESCENDING)
      .setParallelism(1)
      .first(10)

    // calculate top 10 area
    val areaTop10 = input.map(x => (x._2, x._4))
      .sortPartition(0, Order.DESCENDING)
      .setParallelism(1)
      .first(10)

    // calculate top 10 population density
    val desityTop10 = input.map(x => (x._3, x._4))
      .sortPartition(0, Order.DESCENDING)
      .setParallelism(1)
      .first(10)


    populationTop10.writeAsCsv(relativePath + "populationTop10.csv")
    populationTop10.print()
    areaTop10.writeAsCsv(relativePath + "areaTop10.csv")
    areaTop10.print()
    desityTop10.writeAsCsv(relativePath + "densityTop10.csv")
    desityTop10.print()

  }




}