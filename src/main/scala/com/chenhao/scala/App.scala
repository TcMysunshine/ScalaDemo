package com.chenhao.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD;

/**
  * Hello world!
  *
  */
object Test extends App{
  val conf = new SparkConf().setAppName("scalatest").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50))
  )
  val edgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(5L, 2L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3)
  )
  val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
  val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
  val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
  println("vertices filter")
  graph.vertices.filter{case (id,(name, age))=>age>30}.collect.foreach{
    case (id,(name, age)) => println(s"$name is $age")
  }
  graph.triplets.foreach(t=>println(s"src:${t.srcId},${t.srcAttr}," +
    s"dst:${t.dstId},${t.dstAttr}"))
  println("age + 10")
  graph.mapVertices{ case (id,(name, age)) => (id,(name, age + 10))}
    .vertices.collect.foreach(v => println(s"${v._2._2._1} is ${v._2._2._2}"))
//  println("Hello World!")
  val subgraph = graph.subgraph(vpred = (id, vd)=>vd._2>30)
  subgraph.vertices.collect.foreach(v=>println(s"${v._2._1} is ${v._2._2}"))
  println(graph.inDegrees.collect.foreach(e=>println(s"${e._1} + indegree + ${e._2}")))
  println(graph.outDegrees.collect.foreach(e=>println(s"${e._1} + outdegree + ${e._2}")))
  def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)={
    if (a._2>a._1) a else b
  }
  println("max indegree" + graph.inDegrees.reduce(max) + "max outdegree" + graph.outDegrees.reduce(max))
}