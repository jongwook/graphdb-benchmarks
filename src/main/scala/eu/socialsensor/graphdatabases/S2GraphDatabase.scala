package eu.socialsensor.graphdatabases

import java.io.File
import java.util
import java.util.concurrent.Executors

import com.typesafe.config.{Config, ConfigFactory}
import eu.socialsensor.graphdatabases.S2GraphDatabase._
import eu.socialsensor.insert.{S2GraphMassiveInsertion, S2GraphSingleInsertion}
import eu.socialsensor.main.GraphDatabaseType
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, Service, ServiceColumn}
import org.apache.s2graph.core.types.{InnerVal, VertexId, LabelWithDirection}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object S2GraphDatabase {

  val nThreads = Runtime.getRuntime.availableProcessors()
  implicit val context = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nThreads * 2))

  lazy val service: Service = Service.findByName("benchmark").get
  lazy val serviceId: Int = service.id.get
  lazy val label: Label = Label.findByName("benchmark").get
  lazy val labelId: Int = label.id.get
  lazy val column: ServiceColumn = ServiceColumn.find(serviceId, "item_id").get
  lazy val columnId: Int = column.id.get


}

class S2GraphDatabase(config: Config, dbStorageDirectory: File)
  extends GraphDatabaseBase[Iterator[Vertex], Iterator[Edge], Vertex, Edge](GraphDatabaseType.S2GRAPH, dbStorageDirectory) {

  val logger = LoggerFactory.getLogger(getClass)
  var s2: Graph = _
  var mgmt: Management = _
  var label: String = _

  // lifecycle
  override def open(): Unit = {
    val config = ConfigFactory.load()
    s2 = new Graph(config)
    mgmt = new Management(s2)

    mgmt.createService(
      "benchmark",
      config.getString("hbase.zookeeper.quorum"),
      s"benchmark-${config.getString("phase")}",
      1, None,
      config.getString("hbase.table.compression.algorithm")
    ) match {
      case Success(s) => logger.info(s"Created service: $s")
      case Failure(e) => logger.warn(s"Did not create service: $e")
    }

    mgmt.createLabel(
      label = "benchmark",
      srcServiceName = "benchmark",
      srcColumnName = "item_id",
      srcColumnType = "string",
      tgtServiceName = "benchmark",
      tgtColumnName = "item_id",
      tgtColumnType = "string",
      serviceName = "benchmark",
      indices = Seq(),
      props = Seq(),
      consistencyLevel = "weak",
      hTableName = None,
      hTableTTL = None,
      isAsync = false
    ) match {
      case Success(l) => logger.info(s"Created label: $l")
      case Failure(e) => logger.warn(s"Did not create label: $e")
    }

  }

  override def createGraphForSingleLoad(): Unit = open()
  override def createGraphForMassiveLoad(): Unit = open()

  override def shutdown(): Unit = s2.shutdown()
  override def shutdownMassiveGraph(): Unit = s2.shutdown()


  // main benchmark methods
  override def singleModeLoading(dataPath: File, resultsPath: File, scenarioNumber: Int): Unit = {
    val s2graphSingleInsertion = new S2GraphSingleInsertion(this.s2, resultsPath)
    s2graphSingleInsertion.createGraph(dataPath, scenarioNumber)
  }

  override def massiveModeLoading(dataPath: File): Unit = {
    val s2graphMassiveInsertion = new S2GraphMassiveInsertion(this.s2)
    s2graphMassiveInsertion.createGraph(dataPath, 1)
  }

  // OLTP-style queries
  override def getNeighborsOfVertex(v: Vertex): Iterator[Edge] = {
    val future = s2.getEdges(Query(
      vertices = Seq(v),
      steps = IndexedSeq(Step(
        List(QueryParam(
          LabelWithDirection(labelId)
        ))
      ))
    ))
    val result = Await.result(future, 5.seconds)
    result.flatMap {
      _.queryResult.edgeWithScoreLs.map(_.edge)
    }.iterator
  }

  override def getVertex(i: Integer): Vertex = {
    val vertex = Vertex(
      VertexId(columnId, InnerVal.withStr(i.toString, column.schemaVersion))
    )
    val future = s2.getVertices(Seq(vertex))
    val result = Await.result(future, 5.seconds)
    vertex
  }






  // non-query
  override def getOtherVertexFromEdge(r: Edge, oneVertex: Vertex): Vertex = if (oneVertex == r.srcVertex) r.tgtVertex else r.srcVertex
  override def getSrcVertexFromEdge(edge: Edge): Vertex = edge.srcForVertex
  override def getDestVertexFromEdge(edge: Edge): Vertex = edge.tgtForVertex



  // iterator method accessors
  override def getAllEdges: Iterator[Edge] = {
    ??? // TODO
  }

  override def getVertexIterator: Iterator[Vertex] = {
    ??? // TODO
  }

  override def nextEdge(it: Iterator[Edge]): Edge = it.next()
  override def vertexIteratorHasNext(it: Iterator[Vertex]): Boolean = it.hasNext
  override def nextVertex(it: Iterator[Vertex]): Vertex = it.next()
  override def edgeIteratorHasNext(it: Iterator[Edge]): Boolean = it.hasNext



  // OLAP-style querys - suppoerted??
  override def moveNode(from: Int, to: Int): Unit = ???
  override def getNodeCount: Int = ???
  override def delete(): Unit = ???
  override def getGraphWeightSum: Double = ???




  // not being used anywhere; not necessary to implement
  override def getNeighborsIds(nodeId: Int): util.Set[Integer] = ???
  override def getNodeWeight(nodeId: Int): Double = ???
  override def nodeExists(nodeId: Int): Boolean = ???





  // nop
  override def cleanupVertexIterator(it: Iterator[Vertex]): Unit = {}
  override def cleanupEdgeIterator(it: Iterator[Edge]): Unit = {}

  // will not implement
  override def getCommunitiesConnectedToNodeCommunities(nodeCommunities: Int): util.Set[Integer] = ???
  override def getCommunity(nodeCommunity: Int): Int = ???
  override def getCommunityFromNode(nodeId: Int): Int = ???
  override def getCommunitySize(community: Int): Int = ???
  override def getCommunityWeight(community: Int): Double = ???
  override def getEdgesInsideCommunity(nodeCommunity: Int, communityNodes: Int): Double = ???
  override def getNodeCommunityWeight(nodeCommunity: Int): Double = ???
  override def getNodesFromCommunity(community: Int): util.Set[Integer] = ???
  override def getNodesFromNodeCommunity(nodeCommunity: Int): util.Set[Integer] = ???
  override def initCommunityProperty(): Unit = ???
  override def mapCommunities(numberOfCommunities: Int): util.Map[Integer, util.List[Integer]] = ???
  override def reInitializeCommunities(): Int = ???
  override def shortestPath(fromNode: Vertex, node: Integer): Unit = ???
}
