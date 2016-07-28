package eu.socialsensor.insert

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import eu.socialsensor.graphdatabases.S2GraphDatabase._
import eu.socialsensor.main.GraphDatabaseType
import org.apache.s2graph.core.mysqls.LabelMeta
import org.apache.s2graph.core.types.{InnerValLikeWithTs, InnerVal, LabelWithDirection, VertexId}
import org.apache.s2graph.core.{GraphUtil, Edge, Graph, Vertex}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

class S2GraphMassiveInsertion(backend: GraphDatabaseType, graph: Graph) extends InsertionBase[Vertex](backend, null) {

  val logger = LoggerFactory.getLogger(getClass)
  val waiting = new AtomicInteger()

  override protected def getOrCreate(value: String): Vertex = {
    val vertex = Vertex(
      VertexId(columnId, InnerVal.withStr(value, column.schemaVersion))
    )
    Await.result(graph.mutateVertices(Seq(vertex)), 5.seconds)
    vertex
  }

  override protected def relateNodes(src: Vertex, dest: Vertex): Unit = {
    val ts = System.currentTimeMillis()
    val edge = Edge(
      src, dest, label, GraphUtil.directions("out"),
      propsWithTs = Map(LabelMeta.timestamp -> InnerValLikeWithTs.withLong(ts, ts, column.schemaVersion)),
      op = GraphUtil.operations("insertBulk")
    )
    waiting.incrementAndGet()
    graph.mutateEdges(Seq(edge), withWait = false).foreach {
      _ => waiting.decrementAndGet()
    }
  }

  override protected def post(): Unit = {
    while (waiting.get() > 0) {
      logger.info(s"#waiting = ${waiting.get()}")
      Thread.sleep(1000)
    }
  }
}
