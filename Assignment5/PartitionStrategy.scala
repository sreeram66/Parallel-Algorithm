import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}

/**
  * Created by sreeram on 12/5/16.
  */
object PartitionStrategy {

  case object EdgePartition2D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
      val mixingPrime: VertexId = 1125899906842597L
      if (numParts == ceilSqrtNumParts * ceilSqrtNumParts) {
        // Use old method for perfect squared to ensure we get same results
        val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
        val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
        (col * ceilSqrtNumParts + row) % numParts

      } else {
        // Otherwise use new method
        val cols = ceilSqrtNumParts
        val rows = (numParts + cols - 1) / cols
        val lastColRows = numParts - rows * (cols - 1)
        val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
        val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows else lastColRows)).toInt
        col * rows + row

      }
    }
  }

  /**
    * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
    * source.
    */
  case object EdgePartition1D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      (math.abs(src * mixingPrime) % numParts).toInt
    }
  }


  /**
    * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
    * random vertex cut that colocates all same-direction edges between two vertices.
    */
  case object RandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      math.abs((src, dst).hashCode()) % numParts
    }
  }


  /**
    * Assigns edges to partitions by hashing the source and destination vertex IDs in a canonical
    * direction, resulting in a random vertex cut that colocates all edges between two vertices,
    * regardless of direction.
    */
  case object CanonicalRandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      if (src < dst) {
        math.abs((src, dst).hashCode()) % numParts
      } else {
        math.abs((dst, src).hashCode()) % numParts
      }
    }
  }

  /** Returns the PartitionStrategy with the specified name. */
  def fromString(s: String): PartitionStrategy = s match {
    case "RandomVertexCut" => RandomVertexCut
    case "EdgePartition1D" => EdgePartition1D
    case "EdgePartition2D" => EdgePartition2D
    case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
    case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }
}
