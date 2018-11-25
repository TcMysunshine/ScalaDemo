package com.chenhao.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, lib}
import org.apache.spark.rdd.RDD

object SVDPP {
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
      Edge(1L, 2L, 7.0),
      Edge(3L, 2L, 2.0),
      Edge(4L, 2L, 4.0),
      Edge(1L, 4L, 3.0),
      Edge(5L, 7L, 1.0),
      Edge(6L, 7L, 2.0),
      Edge(8L, 9L, 8.0),
      Edge(8L, 10L, 3.0),
      Edge(9L, 10L, 8.0)
    )

    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)

    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    graph.triplets.collect.foreach(println(_))

    println("Community")
//    val lpaGraph = lib.SVDPlusPlus.r
//    val lpaVertices = lpaGraph.vertices
//
//    println("triplet")
//    lpaGraph.triplets.foreach(println(_))
//
//    println("vertices")
//    lpaVertices.collect.foreach(println(_))

    //    println("-------")
    //    var lpa = graph.vertices.join(lpaVertices).map{
    //      case(id, (username, label))=> (username,label)
    //    }
    //
    //    lpa.collect.foreach(println(_))

    sc.stop()
//    val dst:VertexId = 10L
//    lib.ShortestPaths.run(graph,dst )
  }
}
