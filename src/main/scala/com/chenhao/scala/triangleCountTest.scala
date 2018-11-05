package com.chenhao.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
object triangleCountTest {
  def main(args: Array[String]): Unit = {
    val filepath = "/Users/chenhao/Documents/Data/GraphxData/ConnectComponent/"
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, filepath + "follower.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
//    graph.conn
    val triCounts = graph.triangleCount().vertices
    triCounts.collect.foreach(println(_))
    // Join the triangle counts with the usernames
    val users = sc.textFile(filepath + "user.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
  }
}
