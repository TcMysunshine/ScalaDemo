package com.chenhao.scala
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.lib.SVDPlusPlus
import scala.reflect.ClassTag
object LPA {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("scalatest").setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val vertexArray = Array(
//      (1L, ("Alice",13)),
//      (2L, ("Bob",23)),
//      (3L, ("Charlie",45)),
//      (4L, ("David",34)),
//      (5L, ("Ed",18)),
//      (6L, ("Fran",12))
//    )
    val vertexArray = Array(
      (1L, "Alice"),
      (2L, "Bob"),
      (3L, "Charlie"),
      (4L, "David"),
      (5L, "Ed"),
      (6L, "Fran"),
      (7L, "Jack"),
      (8L, "Ma"),
      (9L, "MOLL"),
      (10L, "HHH")
    )
    val edgeArray = Array(
      Edge(1L, 2L, "Game"),
      Edge(3L, 2L, "Food"),
      Edge(4L, 2L, "Game"),
      Edge(5L, 7L, "Beauty"),
      Edge(6L, 7L, "Beauty"),
      Edge(8L, 10L, "Beauty"),
      Edge(9L, 10L, "Food")
    )

    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)
    val graph: Graph[String, String] = Graph(vertexRDD, edgeRDD)
    graph.triplets.collect.foreach(println(_))
//    val graph = GraphLoader.edgeListFile(sc,"/Users/chenhao/Documents/Data/GraphxData/Pregel/web-Google.txt")

    println("Community")
    val lpaGraph = lib.LabelPropagation.run(graph, 40)
    val lpaVertices = lpaGraph.vertices

//    println("triplet")
//    lpaGraph.triplets.foreach(println(_))
//
//    println("vertices")
//    lpaVertices.collect.foreach(println(_))



    graph.vertices.join(lpaVertices).map{
      case(id, (username, label))=> (label,username)
    }.groupByKey().map(t=>t._1+"->"+t._2.mkString(",")).foreach(println(_))

    println("-------")

    var lpa = graph.vertices.join(lpaVertices).map{
      case(id, (username, label))=> (label,id)
    }.groupByKey()
    lpa.map(t=>t._1+"->"+t._2.mkString(",")).foreach(println(_))

    sc.stop()
  }
}
