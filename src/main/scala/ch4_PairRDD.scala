import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Created by kimjun on 16. 9. 28..
 */
object ch4_PairRDD {

  val conf = new SparkConf().setAppName("ch4_PairRDD").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) : Unit = {
    val data = List((1,3), (3, 2), (3, 10))
    val data2 = List((3, 9))

    val dataPersonTall = List(("kim", 183), ("lee", 180), ("kim", 180), ("lee", 160))

//    reduceByKeyTest(data)
//    groupByKeyTest(data)
//    combineByKeyTest(data)
//    mapValuesTest(data)
//    flatMapTest(data)
//    keyValueTest(data)
//    subtractByKeyTest(data, data2)
//    joinTest(data, data2)
//    rightOuterJoinTest(data, data2)
//    leftOuterJoinTest(data, data2)
//      coGroupTest(data, data2)
//        filterTest(data)
//    getAvgTest(dataPersonTall)
//    wordCount()
//    combineByKey(dataPersonTall)
//    countByKeyTest(data)
//    collectAsMapTest(data)
    lookupTest(dataPersonTall)

  }
  def reduceByKeyTest(data: List[(Int, Int)]) : Unit = {
    val rdd = sc.parallelize(data)
    val result = rdd.reduceByKey((x, y) => x + y)

    for (i <- result)
      println(i)

  }

  def groupByKeyTest(data: List[(Int, Int)]) : Unit = {
    val rdd = sc.parallelize(data)
    val result = rdd.groupByKey()

    for (i <- result)
      println(i)

  }

  /**
   * combineByKey 내용 이해 필요.
   */

  def combineByKeyTest(data: List[(Int, Int)]) = {
    val rdd = sc.parallelize(data)
    val result = rdd.combineByKey(
      (v) => (v, 1),
      (acc : (Int, Int), v ) => (acc._1 + v, acc._2 + 1),
      (acc1 : (Int, Int), acc2 : (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .map { case (key, value) => (key, value._1 / value._2.toFloat)}

    result.collectAsMap().foreach(println(_))

  }

  def mapValuesTest(data: List[(Int, Int)]) : Unit = {
    val rdd = sc.parallelize(data)
    val result = rdd.mapValues(x => x + 1)

    for (i <- result)
      println(i)

  }

  /**
   *
   * 각 인자로 쪼개지는 과정이 어떻게 동작하는가?
   *   => 1) 매 항목마다 동작하는가? 그렇다면 중복제거는 ?
   *      2) 해당 항목을 미리 뽑아서 그중 가장 값을 기준으로 동작하는가 ?
   */
  def flatMapTest(data: List[(Int, Int)]) : Unit = {
    val rdd = sc.parallelize(data)
    val result = rdd.flatMapValues(x => x to 5)

    for (i <- result)
      println(i)

  }

  def keyValueTest(data: List[(Int, Int)]) : Unit = {
    val rdd = sc.parallelize(data)
    val key = rdd.keys
    val value = rdd.values
    val sortByKey = rdd.sortByKey()
    val sortByVal = rdd.sortBy(_._2)
    val sortByVal2 = rdd.sortBy(_._1)

    for (i <- key)
      println(i)
    println("-------")
    for (i <- value)
      println(i)
    println("-------")
    for (i <- sortByKey)
      println(i)
    println("-------")
    for (i <- sortByVal)
      println(i)
    println("-------")
    for (i <- sortByVal2)
      println(i)

  }
  
  def subtractByKeyTest(data1 : List[(Int, Int)], data2 : List[(Int, Int)]) {
    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)
    
    val subData = rdd1.subtractByKey(rdd2)

    println("-------")
    for (i <- subData)
      println(i)
  }

  def joinTest(data1 : List[(Int, Int)], data2 : List[(Int, Int)]) {
    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    val join = rdd1.join(rdd2)

    println("-------")
    for (i <- join)
      println(i)
  }

  /**
   *  rightOuter 와 leftOuter는 키의 대상이 되지 않은 함목에서 value를 가져올때 Some(?) 으로 오게됨.
   */
  def rightOuterJoinTest(data1 : List[(Int, Int)], data2 : List[(Int, Int)]) {
    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    val join = rdd1.rightOuterJoin(rdd2)

    println("-------")
    for (i <- join)
      println(i)
  }

  def leftOuterJoinTest(data1 : List[(Int, Int)], data2 : List[(Int, Int)]) {
    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    val join = rdd1.leftOuterJoin(rdd2)

    println("-------")
    for (i <- join)
      println(i)
  }
  /**
   *  value 를 가져올때 CompactBuffer(?) 으로 오게됨.
   *  Why not camel "cogroup" ?
   */
  def coGroupTest(data1 : List[(Int, Int)], data2 : List[(Int, Int)]) {
    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    val cogroup = rdd1.cogroup(rdd2)

    println("-------")
    for (i <- cogroup)
      println(i)
  }

  def filterTest(data1 : List[(Int, Int)]): Unit = {
    val rdd = sc.parallelize(data1)

    val filtered = rdd.filter(x => x._1 == 1)
    println("-------")
    for (i <- filtered)
      println(i)

  }

  def getAvgTest(data1 : List[(String, Int)]) : Unit = {
    val rdd  = sc.parallelize(data1)

    val result = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val result2 =rdd.reduceByKey((x, y) => (x + y))

    println("-------")
    for (i <- result)
      println(i, i._2._1 / i._2._2)

    println("-------")
    for (i <- result2)
      println(i)

  }
  def wordCount(): Unit ={
    val tf = sc.textFile("/Users/kimjun/Documents/mywork/spark-study/build.sbt")
    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
    val counts = splits.reduceByKey((x,y)=> x + y , 4)
    for (i <- counts)
      println(i)
  }

  /**
   *
   * RDD type 에서의  aggregate 와 동일한 과정임.
   *  차이점
   *  1) zero value가 필요없음
   *  2) 결과가 하나의 row가 아닌 각 키별로 나누어짐.
   */
  def combineByKey(data : List[(String, Int)]): Unit = {
    val rdd  = sc.parallelize(data, 1)

    val result = rdd.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 +1),
      (acc1 : (Int, Int), acc2 : (Int, Int)) => (acc1._1 + acc2._2, acc1._2 + acc2._2)
    )
//    println(result)
    for (i <- result)
          println(i)
  }

  def countByKeyTest(data : List[(Int, Int)]): Unit = {
    val rdd = sc.parallelize(data)

    val result = rdd.countByKey()
    for (i <- result)
      println(i)
  }

  def collectAsMapTest(data : List[(Int, Int)]): Unit = {
    val rdd = sc.parallelize(data)
    val result = rdd.collectAsMap()

    println("collectAsMapTest, size:" + rdd.count())
    for (i <- result)
      println(i)
  }

  def lookupTest(data : List[(String, Int)]): Unit = {
    val rdd = sc.parallelize(data)
    val result = rdd.lookup("kim")

    println("lookupTest, size:" + rdd.count())
    for (i <- result)
      println(i)
  }



  class UserId {
    val id = 0
  }

  /**
   * method 정의 시에 '=', {} 가 있고 없고의 차이는 무엇인가?
   */

  class Greeter(var message: String) {
    val name = ""
    println("A greeter is being instantiated")

    message = "I was asked to say " + message

    def SayHi() = println(name + message)
  }

  class UserInfo (var nameVal : String) {
    var name = nameVal
    val sex = "male"
    val topics = Array[String]()
    def setName(nameVal : String) {
      name = nameVal
    }
  }

  class LinkInfo {
    val link =  Array[String]()
    val topics = Array[String]()
  }

  /**
   * Error : could not find implicit value for parameter
   */
  val userData = sc.sequenceFile[UserId, UserInfo]("aaa").persist()
  //partitionBy 를 통해 각 항목을 특정 partition에 종속 시킴으로써 join이 발생할 때 파티션과 userData간의 조인이 발생하지 않는다.
  var userData2 = sc.sequenceFile[UserId, UserInfo]("").partitionBy(new HashPartitioner(100)).persist()

  def test(name : String): Unit = {
    val event = sc.sequenceFile[UserId, LinkInfo](name)
    val joined = userData.join(event)

    val offTopicVisits = joined.filter {
      case (userId, (userInfo, linkInfo)) =>
        !userInfo.topics.contains(linkInfo.topics)
    }.count()

    println("Number of visits to non-subcribed toptics :" + offTopicVisits)

    var userInfo = new UserInfo("aaa")
  }

//  def printData(data : RDD[(Int, Int)]) {
//    println("-------")
//    for (i <- data)
//      println(i)
//  }

}
