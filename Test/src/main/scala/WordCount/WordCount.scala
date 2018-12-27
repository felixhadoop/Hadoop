

package WordCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.sax.BodyContentHandler
import java.io._
import org.apache.spark.input.PortableDataStream

import scala.collection.mutable.ListBuffer

object WordCount {

  var words =  new ListBuffer[String]()
  
  def tikaFunc(a: (String, PortableDataStream)) = {

    println("Processing file: " + a._1.drop(5))
    val file: File = new File(a._1.drop(5))
    val pdfparser: PDFParser = new PDFParser()
    val inputstream: InputStream = new FileInputStream(file)
    val handler: BodyContentHandler = new BodyContentHandler()
    val metadata: Metadata = new Metadata()
    val context: ParseContext = new ParseContext()
    pdfparser.parse(inputstream, handler, metadata, context)
    
    val str = handler.toString().filterNot("\n".toSet)
    words += str

  }

  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PDFWordCount")

    val sc = new SparkContext(conf)
    
    val filesPath = "/home/felixchung/*.pdf"

    val fileData = sc.binaryFiles(filesPath)
    fileData.foreach(x => tikaFunc(x))

    val rddTest = sc.parallelize(words.toList)
    
    rddTest.flatMap { line => //for each line
      line.split(" ") //split the line in word by word.
    }
      .map { word => //for each word
        (word, 1) //Return a key/value tuple, with the word as key and 1 as value
      }
      .reduceByKey(_ + _) //Sum all of the value with same key
      .saveAsTextFile("output.txt") //Save to a text file

    //Stop the Spark context
    sc.stop
  }
}