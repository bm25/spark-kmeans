package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    //val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow-short-2.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    //Call sample(true, 0.1, 0) on scoredPostings(grouped) to downsample initial points
    val scored  = scoredPostings(grouped)// TODO: Test that scored RDD has 2121822 entries
    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)//run kMeans algorithm with initial points (means) returned by sampleVectors()
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  /* Original */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  //Test-1
  //based on stackoverflow-short-1.csv
  //val langs = List("JavaScript", "Java", "PHP", "Python", "C#", "C++", "Objective-C")

  //Test-2
  /* based on stackoverflow-short-2.csv */
  /*
  val langs =
  List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Groovy")
  */

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  /* Original */
  def kmeansKernels = 45

  //Test-1
  //based on stackoverflow-short-1.csv
  //def kmeansKernels = 14 ////based on stackoverflow-short-1.csv

  //Test-2
  //based on stackoverflow-short-2.csv
  //def kmeansKernels = 28 //based on stackoverflow-short-2.csv


  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
        score =          arr(4).toInt,
        tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(posting => posting.postingType == 1)
      .map(question => (question.id.asInstanceOf[QID],question.asInstanceOf[Question]))
    val answers = postings.filter(posting => posting.postingType == 2 && posting.parentId.nonEmpty)
      .map(answer => (answer.parentId.getOrElse(throw new RuntimeException("Empty parentId for the answer#: " + answer.id)),answer.asInstanceOf[Answer]))

    questions.join(answers)
      .groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    grouped.mapValues(group => (
      group.head._1,//replace with Option
      answerHighScore(group.map(postingTuple => postingTuple._2).toArray)//replace with Option
    )
    ).values
  }

  def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
    if (tag.isEmpty) None
    else if (ls.isEmpty) None
    else if (tag.get == ls.head) Some(0) // index: 0
    else {
      val tmp = firstLangInTag(tag, ls.tail)
      tmp match {
        case None => None
        case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
      }
    }
  }

  def getScoredLangIdx(scoredQuestion: (Question, HighScore)): (Int, HighScore) = {
    val firstLang = scoredQuestion._1.tags
      .map(_.split(" ").head)
    val index = firstLangInTag(firstLang, langs)
    (index.getOrElse(0)*langSpread, scoredQuestion._2)
  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {//TODO: check where LangIndex = 0 comes from
    /** Return optional index of first language that occurs in `tags`. */

    scored
      .filter(q => q._1 != null && q._1.tags != null)
      .map(scoredQuestion => getScoredLangIdx(scoredQuestion))
      /*
      .map(scoredQuestion => {
        val firstLang = scoredQuestion._1.tags
          .map(_.split(" ").head)
        val index = firstLangInTag(firstLang, langs)
        (index.getOrElse(0)*langSpread, scoredQuestion._2)//TODO: throw an exception when index is empty
      })
       */
  }

  // http://en.wikipedia.org/wiki/Reservoir_sampling
  def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
    val res = new Array[Int](size)
    val rnd = new util.Random(lang)

    for (i <- 0 until size) {
      assert(iter.hasNext, s"iterator must have at least $size elements")
      res(i) = iter.next
    }

    var i = size.toLong
    while (iter.hasNext) {
      val elt = iter.next
      val j = math.abs(rnd.nextLong) % i
      if (j < size)
        res(j.toInt) = elt
      i += 1
    }

    res
  }

  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {//Array[(LangIndex, HighScore)]

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey
          //.filter(groupedLang => groupedLang._2.size >= perLang )//filter questions without answers (because of provided assertion)
          .flatMap({
            case (lang, vectors) => {
                println("debug1: lang=" + lang + ", vectors.toIterator.size = " + vectors.toIterator.size + ", perLang= " + perLang)
                reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
            }
          })
          //.distinct()//TODO: check if it is overhead
          .collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  def recalculateAvgForCluster(means: Array[(Int, Int)], cluster: (Int, Iterable[(LangIndex, HighScore)])) = {
    println("recalculateAvgForCluster:: before::  idx =" + cluster._1 + ", oldMeanValue = " + means(cluster._1))

    val newClusterAvg = averageVectors(cluster._2)
    means(cluster._1) = newClusterAvg

    println("recalculateAvgForCluster:: after::  newClusterAvg =" + newClusterAvg + ", newMeanValue = " + means(cluster._1))
  }

  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    var newMeans = means.clone() // you need to compute newMeans

    // means - is sub-set of vectors
    val clusteredVectors = vectors.map(p => (findClosest(p, means), p))
    val vectorsByCluster = clusteredVectors.groupByKey()//RDD[( ClosestPointIdxInt, Iterable[(LangIndex, HighScore)] )]

    //idx of means from oldMeans array that go to newMeans array: vectorsByCluster.keys.collect(). TODO: get indexes of old means that should be copied to newMeans array without recalculating average value
    // TODO: Fill in the newMeans array

    val clusterMeansToUpdateAvg = vectorsByCluster.map(cluster => (cluster._1, averageVectors(cluster._2))).collect()

    for (newClusterAvg <- clusterMeansToUpdateAvg) {
      println("recalculateAvgForCluster:: before::  idx =" + newClusterAvg._1 + ", oldMeanValue = " + newMeans(newClusterAvg._1))
      newMeans(newClusterAvg._1) = newClusterAvg._2
      println("recalculateAvgForCluster:: after::  newClusterAvg =" + newClusterAvg + ", newMeanValue = " + newMeans(newClusterAvg._1))
    }

    val  distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point Idx */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()//RDD[( ClosestPointIdxInt, Iterable[(LangIndex, HighScore)] )]

    val median = closestGrouped.mapValues { vs =>
      val mostCommonLangIdx: LangIndex = vs.groupBy(_._1)
        .mapValues(_.size)
        .maxBy(_._2)
        ._1

      val langLabel: String   = langs(mostCommonLangIdx)// most common language in the cluster
    val clusterSize: Int    = vs.size
      val langPercent: Double = vs
        .filter(lang => lang._1 == mostCommonLangIdx)
        .size/clusterSize// percent of the questions in the most common language

      val medianScore: Int    = vs.map(x => x._2)
        .toIterator
        .foldLeft((0.0, 1)) { case ((avg, idx), next) => (avg + (next - avg)/idx, idx + 1) }._1.toInt

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
