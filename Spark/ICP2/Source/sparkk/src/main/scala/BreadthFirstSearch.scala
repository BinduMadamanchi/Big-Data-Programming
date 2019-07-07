import org.apache.spark.{SparkConf, SparkContext}
object BreadthFirstSearch {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val scontext = new SparkContext(conf)
    type A = Int
    type Graph = Map[A, List[A]]

    val g: Graph = Map(1 -> List(2,3,5), 2 -> List(1,3,6), 3 -> List(3,4), 4 -> List(1,3),5 -> List(1,6),6 -> List(1,2))

    def BFS(start: A, g: Graph): List[List[A]] = {
      def BFS0(elems: List[A],visited: List[List[A]]): List[List[A]] = {
        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newNeighbors.isEmpty)
          visited
        else
          BFS0(newNeighbors, newNeighbors :: visited)
      }
      BFS0(List(start),List(List(start))).reverse
    }
    val bfsresult=BFS(1,g )
    println(bfsresult.mkString(","))

  }
}