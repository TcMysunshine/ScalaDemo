package com.chenhao.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, SingleGraph, SingleNode}
import org.graphstream.ui.spriteManager.SpriteManager
//import org.graphstream.ui.swingViewer.Viewer
import org.graphstream.ui.spriteManager.Sprite
object Visualization {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("GraphStreamDemo")
      .set("spark.master", "local[*]")

    val sc = new SparkContext(sparkConf)

    val graph: SingleGraph = new SingleGraph("graphDemo")

    val vertices: RDD[(VertexId, String)] = sc.parallelize(List(
      (1L, "A"),
      (2L, "B"),
      (3L, "C"),
      (4L, "D"),
      (5L, "E"),
      (6L, "F"),
      (7L, "G")))

    val edges: RDD[Edge[String]] = sc.parallelize(List(
      Edge(1L, 2L, "1-2"),
      Edge(1L, 3L, "1-3"),
      Edge(2L, 4L, "2-4"),
      Edge(3L, 5L, "3-5"),
      Edge(3L, 6L, "3-6"),
      Edge(5L, 7L, "5-7"),
      Edge(6L, 7L, "6-7")))

    val srcGraph = Graph(vertices, edges)

    graph.setAttribute("ui.stylesheet", "url(file:/Users/chenhao/Documents/Data/stylesheet)")
    graph.setAttribute("ui.quality")
    graph.setAttribute("ui.antialias")

    //    load the graphx vertices into GraphStream
    for ((id, _) <- srcGraph.vertices.collect()){
      val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
    }

    //    load the graphx edges into GraphStream edges
    for (Edge(x, y, _) <- srcGraph.edges.collect()) {
      val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }
    import scala.collection.JavaConversions._
    for (node <- graph) {
//      println(node.getLabel("label"))
      node.addAttribute("ui.label", node.getId)
      node.setAttribute("ui.class", "marked")
    }
    //    graph.
    val viewer = graph.display()

  }
}




