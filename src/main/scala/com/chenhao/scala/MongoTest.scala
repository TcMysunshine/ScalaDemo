package com.chenhao.scala
import com.chenhao.scala.Test.{edgeArray, sc, vertexArray}
import com.mongodb.casbah.Imports._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
object MongoTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("scalatest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // To directly connect to the default server localhost on port 27017
    val uri = MongoClientURI("mongodb://localhost:27017/")
    val mongoClient = MongoClient(uri)
    val db = mongoClient("biliSpark")
    val allmonth_play_dm = db("allmonth_play_dm")
    val query = "dm" $gt 1000000
//    allmonth_play_dm.f
    val result = allmonth_play_dm.find(query).sort(MongoDBObject("dm" -> -1))
//    result.foreach(println(_))
//    case class v(id:Long,tv:String)
    val vertexArr =  ArrayBuffer[(Long,String)]()
    val edgeArr =  ArrayBuffer[Edge[Double]]()
    result.foreach(
      t => {
        print(t.get("dm") + "->")
        println(t.get("tv"))
//        val temp= new v
        val tempVer = (t.get("dm").toString.toLong,t.get("tv").toString)
        val tempEdge = Edge(t.get("dm").toString.toLong,t.get("play").toString.toLong,1D)
        vertexArr += tempVer
        edgeArr += tempEdge
      }
    )
    println("Vertex")
    vertexArr.foreach(println(_))
    edgeArr.foreach(println(_))
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArr)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArr)
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD,"Unknown")
    graph.vertices.collect.foreach(println(_))
//    val graph = Graph()

  }

}
