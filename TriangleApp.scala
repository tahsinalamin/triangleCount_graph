import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TriangleApp {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val t1 = System.nanoTime
    val logFile = "tahsin/a.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("TriangleApplication")
    val sc = new SparkContext(conf)
    var graph = GraphLoader.edgeListFile(sc, "/graph/minigraph.txt",true).partitionBy(PartitionStrategy.RandomVertexCut)
    var triCounts = graph.triangleCount().vertices.collect()
    //triCounts.collect().foreach(println)
    var total_triangle=0
    for (i<-0 to (triCounts.length-1))
    {
        val temp=triCounts(i)
        total_triangle+=temp._2
    }
    println("Total Number of Triangles are..")
    println(total_triangle)
    val duration = (System.nanoTime - t1) / 1e9d
    println("The program finishes in .....")
    println(duration)
    sc.stop()
  }
}
