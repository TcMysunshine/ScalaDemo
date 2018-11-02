package com.chenhao.scala
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object PregelTest {
  def main(args: Array[String]): Unit = {
    val filepath = "/Users/chenhao/Documents/Data/GraphxData/Pregel/"
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, filepath + "web-Google.txt")
    graph.vertices.take(10).foreach(println(_))
    val sourceId:VertexId = 0
    val initialGraph = graph.mapVertices((id,_) => if(id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id,dist,newDist) => math.min(dist,newDist),
      triplet => {
        if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
          Iterator((triplet.dstId,triplet.srcAttr+triplet.dstAttr))
        }
        else{
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b)
    )
    sssp.vertices.take(10).mkString("\n")
  }
}
