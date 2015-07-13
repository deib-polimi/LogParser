/**
 *
 */
package parser

import scala.io.Source

import java.io.File
import java.io.FileWriter

/**
 * @author Alessandro
 *
 */
object Parser {

    def apply (filename : String) : (Durations, Sequence) = {
	    println("Starting " + filename)
	    val lines = Source.fromFile(filename).getLines();

	    val start = System.currentTimeMillis();
	    val finalStatus = lines.foldLeft(Start : Status)(_ next _);
	    val stop = System.currentTimeMillis();
	    val i = new File(filename).length;
	    println("Finished in " + ((stop - start).toDouble / 1000) + " s");
	    val ratio = i.toLong * 1000 / (stop-start);
	    println(i + ": " + ratio/1000 + "KB/s")
	    (Durations (finalStatus.durations), Sequence (finalStatus.vertices))
    }

    def apply(files : Seq[String]) : (String, String) = {
	    val (durations, vertices) = files.map (Parser (_)).unzip
	    (durations mkString "\n" + "\n", vertices mkString "" + "\n")
    }

    def parse (path : String): Unit = {
	    val sourceDir = new File (path).getAbsoluteFile
      val dataDir = new File (sourceDir, "data")
      dataDir.mkdir

      val appDurationIn = new File (sourceDir, "appDuration.txt")
	    val appDurationOut = new File (dataDir, "appDuration.txt")
      copyFile(appDurationIn, appDurationOut)

      val inputFiles = sourceDir.listFiles ().sortBy (_.getName)
        .map (_.getPath).filter (_.endsWith (".AMLOG.txt")).toSeq
      val (durationContent, verticesContent) = Parser (inputFiles)
	    writeToFile (durationContent, new File (dataDir, "taskDurationLO.txt"))
      writeToFile (verticesContent, new File (dataDir, "vertexOrder.txt"))
    }

    protected def writeToFile (content: String, file: File): Unit = {
      file.delete
      val out = new FileWriter (file)
      out write content
      out.flush
      out.close
    }

    protected def copyFile (from: File, to: File): Unit = {
      val content = Source.fromFile (from).mkString
      to.delete
      val copy = new FileWriter(to)
      copy.write(content)
      copy.flush()
      copy.close()
    }

    def main(args: Array[String]): Unit = {
      var directory = "/workspace/RC/5_80_R3/fetched/R3/"
      if (args.length > 0) directory = args(0)
	    Parser parse directory
    }
}