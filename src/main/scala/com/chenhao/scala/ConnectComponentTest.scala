package com.chenhao.scala

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
object ConnectComponentTest {
  def main(args: Array[String]): Unit = {
//    #屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val filepath = "/Users/chenhao/Documents/Data/GraphxData/ConnectComponent/"
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    //读取followers.txt文件创建图
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc,filepath + "follower.txt")
    graph.vertices.collect.foreach(println(_))
    //计算连通体
    println("ConnectedComponents")
    val components: Graph[VertexId, Int] = graph.connectedComponents()
//    val components: Graph[VertexId, Int] = graph.stronglyConnectedComponents(10)
    val vertices: VertexRDD[VertexId] = components.vertices
    vertices.foreach(println(_))
    /**
      * vertices：
      * (4,1)
      * (1,1)
      * (6,3)
      * (3,3)
      * (7,3)
      * (2,1)
      * 是一个tuple类型，key分别为所有的顶点id，value为key所在的连通体id(连通体中顶点id最小值)
      */
    //读取users.txt文件转化为(key,value)形式
    val users: RDD[(VertexId, String)] = sc.textFile(filepath + "user.txt").map(line => {
      val fields: Array[String] = line.split(",")
      (fields(0).toLong, fields(1))
    })
    /**
      * users:
      * (1,BarackObama)
      * (2,ladygaga)
      * (3,jeresig)
      * (4,justinbieber)
      * (6,matei_zaharia)
      * (7,odersky)
      * (8,anonsys)
      * (
      */
    users.join(vertices).map{
      case(id,(username,vertices))=>(vertices,username)
    }.groupByKey().map(t=>{
      t._1+"->"+t._2.mkString(",")
    }).foreach(println(_))
    val subgraph = graph.subgraph(epred => epred.dstId ==2  )
    subgraph.triplets.collect.foreach(println(_))
    /**
      * 得到结果为：
      * 1->justinbieber,BarackObama,ladygaga
      * 3->matei_zaharia,jeresig,odersky
      */

  }

}
