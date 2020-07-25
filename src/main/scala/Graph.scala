import org.apache.spark.graphx.{Graph=>Graph1, VertexId,Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object GraphComponents {
  def main ( args: Array[String] ) {
  	val conf = new SparkConf().setAppName("Graph")
  	val sc = new SparkContext(conf)

  	val graph =sc.textFile(args(0)).map(line => {val (vertex,adj)=line.split(",").splitAt(1)
                                         (vertex(0).toLong,adj.toList.map(_.toLong))})

  	val edge = graph.flatMap(a=> a._2.map(b=>(a._1,b))).map(node=>Edge(node._1,node._2,node._1))

  	val newGraph: Graph1[Long,Long]=Graph1.fromEdges(edge,"defaultProperty").mapVertices((id,_)=>id)

  	val newGraph1 = newGraph.pregel(Long.MaxValue,5)((id,PrevG,newG)=> math.min(PrevG,newG),
        triplet=>{
            if(triplet.attr<triplet.dstAttr){
              Iterator((triplet.dstId,triplet.attr))
            }else if((triplet.srcAttr<triplet.attr)){
              Iterator((triplet.dstId,triplet.srcAttr))
            }else{
              Iterator.empty
            }
        },(a1,a2)=>math.min(a1,a2))

  	val finalGraph = newGraph1.vertices.map(newGraph=>(newGraph._2,1)).reduceByKey(_ + _).sortByKey().map(a=>a._1.toString+" "+a._2.toString )
	finalGraph.collect().foreach(println)
  }
}
