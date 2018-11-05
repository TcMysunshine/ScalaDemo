package com.chenhao.scala

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object pageRank {
  def main(args: Array[String]): Unit = {
    val filepath = "/Users/chenhao/Documents/Data/GraphxData/ConnectComponent/"
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, filepath + "follower.txt")
    // Run PageRan
    val ranks = graph.pageRank(0.0001).vertices
//    graph.
    // Join the ranks with the usernames
    val users = sc.textFile(filepath + "user.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }
}
