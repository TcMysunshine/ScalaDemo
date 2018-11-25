package com.chenhao.scala

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, StdIn}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, lib}
import org.apache.spark.rdd.RDD
object Duplicate2Way {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val filepath = "/Users/chenhao/Documents/Data/GraphxData/bili"
    val conf = new SparkConf().setAppName("scalatest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    print("请输入文件夹:" )
    val filepath = StdIn.readLine()
//    val filepath="/Users/chenhao/Documents/CloudCompute/data/GraphX"
    val datafilepath = filepath + "/data.txt"
    val variableFilepath = filepath + "/variables.txt"
    val data = Source.fromFile(datafilepath)
    val edgeArr = ArrayBuffer[Edge[Double]]()
    for (edge <- data.getLines()){
      val e = edge.split(",")
      val tempEdge = Edge(e(0).substring(2).toLong,e(1).substring(2).toLong,1D)
      edgeArr+=tempEdge
    }
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArr)
    val graph = Graph.fromEdges(edgeRDD,"chenhao")

//    graph.edges.collect().foreach(println(_))
//    ---------------------------------------------
//    重复边和双向边Start
    val distinctGraphEdges = graph.edges.collect().distinct
    val totalEdge = graph.edges.count()
    val notDuplicateEdge = graph.edges.distinct().count()
    println(totalEdge.toString + ":"  + notDuplicateEdge.toString)
    val duplicateEdge = totalEdge - notDuplicateEdge
    println("重复边" + duplicateEdge.toString)
    val graphReverse = graph.reverse
    val distinctGraphReverseEdges = graphReverse.edges.collect().distinct
    var twoWayGraphEdge = distinctGraphEdges.intersect(distinctGraphReverseEdges)
//    twoWayGraphEdge.foreach(println(_))
    var twoWayEdge = twoWayGraphEdge.count(t => true) / 2
    println("双向边" + twoWayEdge.toString)
//    重复边和双向边END
//    ---------------------------------------------

    val pw = new PrintWriter(filepath + "/MF1832013.txt")
    pw.write(twoWayEdge.toString + "\n")
    pw.write(duplicateEdge.toString + "\n")

    val variableData = Source.fromFile(variableFilepath)
    val variableLines = variableData.getLines().toList
    val sourceID = variableLines(0).substring(2).toLong
    val targetID = variableLines(1).substring(2).toLong
    println(sourceID.toString +":"+ targetID.toString)
    //求最短路径
    val graphRidDuplicate = Graph.fromEdges(graph.edges.distinct(),"chenhao")
    println("边" + graphRidDuplicate.edges.count().toString)
    println("顶点" + graphRidDuplicate.vertices.count().toString)
    val landmarks = Seq(targetID)
    val sG = lib.ShortestPaths.run(graphRidDuplicate, landmarks)
//    sG.vertices.collect().foreach(println(_))
    val x = sG.vertices.collect().filter(v=>{
      v._1==sourceID
    }).map(t=>t._2.get(targetID)).mkString
    if(x==""||x==null) {
      pw.write("没有最短路径"+"\n")
      //    sG.edges.collect().foreach(println(_))

      //    println(sssp.vertices.filter()

    }
    else{
      val lIndex = x.indexOf("(")
      val rIndex = x.indexOf(")")
      //    println(x+":"+ lIndex.toString +":"+ rIndex.toString)
      val sp = x.substring(lIndex + 1, rIndex)
      pw.write(sp + "\n")
    }
    pw.close()
  }
}
