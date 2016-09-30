/**
 * Created by kimjun on 16. 9. 22..
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Giri R Varatharajan on 9/8/2015.
 */
object ch3_RDD {


  class AAA(val query: String) {
    def isMatch(s : String):  Boolean = {
      s.contains(query)
    }

    def getMatch(rdd: RDD[String]) : RDD[Boolean] = {
      // isMatch == this.isMatch, this의 모든 것이 딸려감..
      // 이와 같은 inner 클래스의 경우 딸려가는 범위가 당연히 해당 class내 이겟지?
      rdd.map(isMatch)
    }
    def getMatchRef(rdd: RDD[String]) : RDD[Array[String]] = {
      // query는 this.query 이므로 this 전체가 딸려감
      rdd.map(x => x.split(query))
    }
    def getMatchNoRef(rdd: RDD[String]) : RDD[Array[String]] = {
      val query_ = this.query // query부분만 저장해서 전달
      rdd.map(x => x.split(query_))
    }
  }

  def main(args:Array[String]) : Unit = {
//    wordCount(args)
//    filterTest("/Users/kimjun/py_test.csv")
//    intSquare(Array(1, 2, 4, 5))
//    divid(List("seojey is very hansome", "and he want to intelligent", "but not yet get smart"))
//    sampleTest(List(1, 2, 4, 5))
//    intersectionTest(List(1, 2, 4, 5), List(1, 2, 3, 4))
//    subtractTest(List(1, 2, 4, 5), List(1, 2, 3, 4))
//    cateTest(List(1, 2, 4, 5), List(1, 2, 3, 4))
//    reduceTest()
    aggregateTest()
//    countTest()
//    foldTest()
//    persistTest()

  }

  val conf = new SparkConf().setAppName("ch3_RDD").setMaster("local[*]")
  val sc = new SparkContext(conf)


  def filterTest(fileName: String): Unit = {
    val tf = sc.textFile(fileName)

    val errorRDD = tf.filter(line => line.contains("북도"))
    val warRDD = tf.filter(line => line.contains("경상"))
    val union = errorRDD.union(warRDD)

    println("------- not use collect")
    union.foreach(println)
    println("------- use collect")
    union.collect().foreach(println)

  }


  /**
   *
   * @param numberArr: integer list array
   *
   * - input type에 대한 제한이 존재함. 이유 Number는 연산자 오버로딩이 되어있지 않아서... 당연한 결과다.
   * ex) Number X
   *     Integer 0
   *
   */
  def intSquare(numberArr : Array[Integer]): Unit = {
    val input = sc.parallelize(numberArr)
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))
  }

  /**
   *
   * @param list
   * -foreach 의 경우 random 하게 결과가 추출됨.
   */
  def divid(list: List[String]): Unit = {
    val lines = sc.parallelize(list)
    val result = lines.flatMap(line => line.split(" "))
    println(result.collect().mkString(","))
    result.foreach(println)

  }

  /**
   *
   * @param list
   */
//  def divid2(list: List[Integer]): Unit = {
//    val number = sc.parallelize(list)
//    val result = number.flatMap(n => n.to(3) )
//    println(result.collect().mkString(","))
//    result.foreach(println)
//    println(result.first())
//  }

  def sampleTest(list : List[Integer]) : Unit = {
    val rdd = sc.parallelize(1 to 10000)
    val trueSamples = rdd.sample(true, 0.5)
    val falseSamples = rdd.sample(false, 0.5)

//    rdd.foreach(println)
//    trueSamples.foreach(println)
//    falseSamples.foreach(println)
    println(trueSamples.count())
    println(falseSamples.count())

  }

  def intersectionTest(list1 : List[Integer], list2 : List[Integer]): Unit = {
    val tf1 = sc.parallelize(list1)
    val tf2 = sc.parallelize(list2)

    val intersection = tf1.intersection(tf2)

    intersection.foreach(println)

  }

  def subtractTest(list1 : List[Integer], list2 : List[Integer]): Unit = {
    val tf1 = sc.parallelize(list1)
    val tf2 = sc.parallelize(list2)

    val subtract = tf1.subtract(tf2)

    subtract.foreach(println)

  }

  def cateTest(list1 : List[Integer], list2 : List[Integer]): Unit = {
    val tf1 = sc.parallelize(list1)
    val tf2 = sc.parallelize(list2)

    val cartesian = tf1.cartesian(tf2)

    cartesian.foreach(println)

  }

  def reduceTest(): Unit = {

    val a = sc.parallelize(List(1, 2, 3, 4)).collect()
    val c = a.reduce((x, y) => x - y)

    println("result :" + c)
  }

  def foldTest(): Unit = {

    val a = sc.parallelize(List(1, 2, 3, 4)).collect()
    val c = a.fold(1)((x, y) => x + y)

    println("result :" + c)
  }

  /**
   * aggregate 의 동작 과정이 이해되지 않음.
   */
  def aggregateTest(): Unit = {
    val input = sc.parallelize(List(1, 2, 3, 3), 1)
    val result = input.aggregate((0,0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2)
    )
    println("-------")
    //    for (i <- result)
    //
    println(result)
//      println(i)
  }
//  def  aggTest(): Unit = {
//    val input = sc.parallelize(List(1,2,3,4,5,6), 2)
//
//    // lets first print out the contents of the RDD with partition labels
////    def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
////      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
////    }
//
//    input.mapPartitionsWithIndex(myfunc).collect
//
//
//    .aggregate(0)(math.max(_, _), _ + _)
//    res40: Int = 9
//
//    // This example returns 16 since the initial value is 5
//    // reduce of partition 0 will be max(5, 1, 2, 3) = 5
//    // reduce of partition 1 will be max(5, 4, 5, 6) = 6
//    // final reduce across partitions will be 5 + 5 + 6 = 16
//    // note the final reduce include the initial value
//    z.aggregate(5)(math.max(_, _), _ + _)
//    res29: Int = 16
//
//
//    val z = sc.parallelize(List("a","b","c","d","e","f"),2)
//
//    //lets first print out the contents of the RDD with partition labels
//    def myfunc(index: Int, iter: Iterator[(String)]) : Iterator[String] = {
//      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
//    }
//
//    z.mapPartitionsWithIndex(myfunc).collect
//    res31: Array[String] = Array([partID:0, val: a], [partID:0, val: b], [partID:0, val: c], [partID:1, val: d], [partID:1, val: e], [partID:1, val: f])
//
//    z.aggregate("")(_ + _, _+_)
//    res115: String = abcdef
//
//    // See here how the initial value "x" is applied three times.
//    //  - once for each partition
//    //  - once when combining all the partitions in the second reduce function.
//    z.aggregate("x")(_ + _, _+_)
//    res116: String = xxdefxabc
//
//    // Below are some more advanced examples. Some are quite tricky to work out.
//
//    val z = sc.parallelize(List("12","23","345","4567"),2)
//    z.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)
//    res141: String = 42
//
//    z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
//    res142: String = 11
//
//    val z = sc.parallelize(List("12","23","345",""),2)
//    z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
//    res143: String = 10
//  }

  def countTest(): Unit = {
    val rdd = sc.parallelize(1 to 100, 3)
    val take =  rdd.take(10)
    val top = rdd.top(10)
    val takeSample = rdd.takeSample(false, 200)
    val takeOrdered = rdd.takeOrdered(10)

    println("---take--- ")
    for (i <- take)
      println(i)

    println("---top--- ")
    for (i <- top)
      println(i)
    println("---takeSample--- ")
    for (i <- takeSample)
      println(i)

    println("---takeOrdered--- ")
    for (i <- takeOrdered)
      println(i)

  }
  def persistTest(): Unit = {
    val rdd = sc.parallelize(1 to 100)
//76, 66
//    var st: Long = System.currentTimeMillis
//    val result_1 = rdd.map(x => x * x)
//    result_1.persist(StorageLevel.MEMORY_ONLY)
//    println(result_1)
//    var et: Long = System.currentTimeMillis
//    println("perf time (MEMORY_ONLY): " + (et - st))

//50, 73
//    var st = System.currentTimeMillis
//    val result_1_1= rdd.map(x => x * x)
//    result_1_1.persist(StorageLevel.MEMORY_ONLY_2)
//    println(result_1_1)
//    var et =System.currentTimeMillis
//    println("perf time (MEMORY_ONLY_2): " + (et - st))

//79, 64
//    var st = System.currentTimeMillis
//    val result_2= rdd.map(x => x * x)
//    result_2.persist(StorageLevel.MEMORY_ONLY_SER)
//    println(result_2)
//    var et =System.currentTimeMillis
//    println("perf time (MEMORY_ONLY_SER): " + (et - st))

//58, 112
//    var st = System.currentTimeMillis
//    val result_3= rdd.map(x => x * x)
//    result_3.persist(StorageLevel.MEMORY_AND_DISK)
//    println(result_3)
//    var et = System.currentTimeMillis
//    println("perf time (MEMORY_AND_DISK): " + (et - st))
//102, 64
//    var st = System.currentTimeMillis
//    val result_4= rdd.map(x => x * x)
//    result_4.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    println(result_4)
//    var et =System.currentTimeMillis
//    println("perf time (MEMORY_AND_DISK_SER): " + (et - st))


//67, 69
//    var st = System.currentTimeMillis
//    val result_6= rdd.map(x => x * x)
//    result_6.persist(StorageLevel.DISK_ONLY_2)
//    println(result_6)
//    var et = System.currentTimeMillis
//    println("perf time (DISK_ONLY_2): " + (et - st))
//75, 85
    var st = System.currentTimeMillis
    val result_5= rdd.map(x => x * x)
    result_5.persist(StorageLevel.DISK_ONLY)
    println(result_5)
    var et =System.currentTimeMillis
    println("perf time (DISK_ONLY): " + (et - st))

  }
}
