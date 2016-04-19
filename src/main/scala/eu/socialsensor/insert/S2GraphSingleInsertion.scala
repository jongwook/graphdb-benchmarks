package eu.socialsensor.insert

import java.io.File

import eu.socialsensor.graphdatabases.S2GraphDatabase._
import eu.socialsensor.main.GraphDatabaseType
import org.apache.s2graph.core.types.{LabelWithDirection, InnerVal, VertexId}
import org.apache.s2graph.core.{Edge, Graph, Vertex}

import scala.concurrent.Await
import scala.concurrent.duration._

class S2GraphSingleInsertion(graph: Graph, resultsPath: File) extends InsertionBase[Vertex](GraphDatabaseType.S2GRAPH, resultsPath) {
  override protected def getOrCreate(value: String): Vertex = {
    val vertex = Vertex(
      VertexId(columnId, InnerVal.withStr(value, column.schemaVersion))
    )
    Await.result(graph.mutateVertices(Seq(vertex)), 5.seconds)
    vertex
  }

  override protected def relateNodes(src: Vertex, dest: Vertex): Unit = {
    val edge = Edge(
      src, dest, LabelWithDirection(labelId, 0), propsWithTs = Map()
    )
  }
}
