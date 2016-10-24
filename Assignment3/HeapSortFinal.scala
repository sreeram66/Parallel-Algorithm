/**
  * Created by sreeram on 10/24/16.
  */

import scala.util._
import scala.collection.mutable._
import scala.util.Random
object HeapSortFinal {

  private val randGen = new Random()

  def main(args: Array[String]) {
    val arraySize = args.first.toInt
    val repeat = args(1).toInt
    var times = perform(repeat, arraySize)
    println("Average: "+averageOf(times)+" ms for "+times.size+" runs.")
  }

  private def heapSort(arr:Array[Int]):Array[Int] = {
    buildHeap(arr)
    (arr.length-1 until 0 by -1).foreach( i => {
      swap(arr, 0, i)
      heapify(arr, 0, i)
    })
    arr
  }

  def buildHeap(arr:Array[Int]) {
    ((arr.length/2.0D).floor.toInt-1 until -1 by -1).foreach( i => heapify(arr, i, arr.length) )
  }

  def heapify(arr:Array[Int], idx:Int, max:Int) {
    val l = left(idx)
    val r = right(idx)
    var largest = if (l < max && arr(l) > arr(idx)) l else idx
    largest = if (r < max && arr(r) > arr(largest)) r else largest
    if (largest != idx) {
      swap(arr, idx, largest)
      heapify(arr, largest, max)
    }
  }

  private def parent(idx:Int):Int = (idx/2.0D).floor.toInt
  private def left(idx:Int):Int = 2*idx+1
  private def right(idx:Int):Int = (2*idx)+2

  def swap(s:Array[Int], i: Int, j: Int):Unit = {
    val v = s(i);
    s(i) = s(j);
    s(j) = v
  }


  private def perform(times:Int, initListSize:Int):ListBuffer[Int] = {
    val benchmarks:ListBuffer[Int] = new ListBuffer[Int]
    (0 until times).foreach(idx => {
      val arr:Array[Int] = initArrayWith(initListSize)
      val start = System.currentTimeMillis()
      heapSort(arr)
      val end = System.currentTimeMillis()
      benchmarks += (end-start).toInt
    });
    benchmarks
  }

  def initArrayWith(limit:Int):Array[Int] = {
    val list:ListBuffer[Int] = new ListBuffer()
    val randGen = new Random()
    (0 until limit).foreach( i => list += randGen.nextInt(1000000) )
    return list.toArray
  }

  def averageOf(benchmarks:ListBuffer[Int]):Long = {
    var sortedBm:Array[Int] = benchmarks.toArray
    heapSort(sortedBm)
    var sum:Int = 0;
    val sumFunc = (t:Int) => sum += t
    // Get rid of best and worst if we ran it more than twice
    if (sortedBm.length > 2)
      sortedBm.slice(1,sortedBm.size-2).foreach(sumFunc)
    else
      sortedBm.foreach(sumFunc)
    return sum/sortedBm.length
  }

}
