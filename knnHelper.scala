package scalaKNN

import scala.math.sqrt
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 
import scala.io
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

class knnHelper{
    private val trainSet = readFile("small.txt")
    private val k = 3 
    def readFile(filename: String): Array[String] = {
      val bufferedSource = io.Source.fromFile(filename)
      val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
      lines
    }
    
    def findDistance(row1: Array[String], row2: Array[String]): Double = {
      var distance = 0.0
     
      for ( i <- 0 until row1.length-1){
        val diff = row1(i).toDouble - row2(i).toDouble
        distance += diff * diff
      }
      sqrt(distance)
    }
    
    def knnScala(testRow: Array[String], filename: String, k_str: String): Int ={
      val trainSet = readFile(filename)
      val k = k_str.toInt  
      //if i want to know neighbors class
      //var neighs= ArrayBuffer.fill(k)(0) 
      var dists = ArrayBuffer.fill(k)(Float.MaxValue)      
      var class_arr = ArrayBuffer.fill(k)(0)
      var dist = 0.0
      var max_idx = 0
      for(i <- 0 until trainSet.length){
        breakable {
        if(i == testRow.last.toInt) break
        //create trainRow then find distance
        val trainRow = trainSet(i).split(",")
        dist = findDistance(trainRow, testRow)
        //find furthest neighbor
        for(j <- 0 until k){
          if(dists(j) > dists(max_idx)){
            max_idx = j
          }
        }
        //replace furthest neighbor with nearer neighbor
        if(dist < dists(max_idx)){
          dists(max_idx) = dist.toFloat
          class_arr(max_idx) = trainRow(trainRow.length-1).toInt          
        } 
        }
      }
      //vote on class
      var max_count = 0
      var my_class = 0
      for(i <- 0 until k){
        var count = 0;
        for(j <- 0 until k){
          if(class_arr(i) == class_arr(j)){
            count += 1
          }
          if(count > max_count){
            max_count = count
            my_class = class_arr(i)
          }
        }
      }       
      my_class
    }
    
}