/* Copyright 2015 Alessandro Maria Rizzi
 * Copyright 2016 Eugenio Gianniti
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parser

import java.io.{File, FileWriter}

import scala.io.Source

object Parser {

  private val USAGE =
    """usage:
      |  LogParser -2|-3 directory""".stripMargin
  private val WARN_EMPTY_INPUT = "WARNING: possibly empty input file"
  private val WRONG_INPUT = "ERROR: wrong input argument"

  private def apply (filename : String, regexVersion : StatusRegex): (StartEnd,
    Durations, StartEnd, Durations, Sequence, VertexListOfTasks, TaskNodes,
    TaskContainers, ShuffleBytes) = {
    println(s"Starting $filename")
    val lines = Source.fromFile(filename).getLines()

    val start = System.currentTimeMillis()
    val finalStatus = ({Start(regexVersion) : Status} /: lines) (_ next _)
    val stop = System.currentTimeMillis()
    val fileSize = new File(filename).length
    val difference = stop - start
    val elapsed = difference.toDouble / 1000
    println(s"Finished in $elapsed s")
    val ratioKB = if (difference > 0) fileSize.toLong / difference else 0l
    println(s"$fileSize: $ratioKB KB/s")
    if (ratioKB == 0) Console.err println WARN_EMPTY_INPUT
    (StartEnd (finalStatus.times), Durations (finalStatus.times),
      StartEnd (finalStatus.shuffleTimes), Durations (finalStatus.shuffleTimes),
      Sequence (finalStatus.vertices),
      VertexListOfTasks (finalStatus.taskToVertices, finalStatus.taskOrder),
      TaskNodes (finalStatus.containerToNodes, finalStatus.taskToContainers,
        finalStatus.taskOrder),
      TaskContainers (finalStatus.taskToContainers, finalStatus.taskOrder),
      ShuffleBytes (finalStatus.shuffleBytes))
  }

  private def apply (files : Seq[String], regexVersion : StatusRegex): (String,
    String, String, String, String, String, String, String, String) = {
    val (taskStartEnds, taskDurations, shuffleStartEnds, shuffleDurations,
    vertices, listsOfTasks, taskNodes, taskContainers, shuffleBytes) =
      files.map (Parser (_, regexVersion))
        .foldLeft ((Seq (): Seq[StartEnd], Seq (): Seq[Durations],
          Seq (): Seq[StartEnd], Seq (): Seq[Durations],
          Seq (): Seq[Sequence], Seq (): Seq[VertexListOfTasks],
          Seq (): Seq[TaskNodes], Seq (): Seq[TaskContainers],
          Seq (): Seq[ShuffleBytes]))
        {(lists, tuple) => (lists._1 :+ tuple._1, lists._2 :+ tuple._2,
          lists._3 :+ tuple._3, lists._4 :+ tuple._4,
          lists._5 :+ tuple._5, lists._6 :+ tuple._6,
          lists._7 :+ tuple._7, lists._8 :+ tuple._8,
          lists._9 :+ tuple._9)}
    (taskStartEnds mkString "\n\n", taskDurations mkString "\n\n",
      shuffleStartEnds mkString "\n\n", shuffleDurations mkString "\n\n",
      vertices mkString "\n", listsOfTasks mkString "\n\n",
      taskNodes mkString "\n\n", taskContainers mkString "\n\n",
      shuffleBytes mkString "\n\n")
  }

  private def parse (path : String, regexVersion : StatusRegex): Unit = {
    val sourceDir = new File (path).getAbsoluteFile
    val dataDir = new File (sourceDir, "data")
    dataDir.mkdir

    val appDurationIn = new File (sourceDir, "appDuration.txt")
    val appDurationOut = new File (dataDir, "appDuration.txt")
    copyFile(appDurationIn, appDurationOut)

    val inputFiles = sourceDir.listFiles ().sortBy (_.getName)
      .map (_.getPath).filter (_.endsWith (".AMLOG.txt")).toSeq
    val (taskStartEndContent, taskDurationContent, shuffleStartEndContent,
    shuffleDurationContent, verticesContent, listOfTasksContent,
    taskNodesContent, taskContainersContent, shuffleBytesContent) =
      Parser (inputFiles, regexVersion)
    writeToFile (taskStartEndContent, new File (dataDir, "taskStartEnd.txt"))
    writeToFile (taskDurationContent, new File (dataDir, "taskDurationLO.txt"))
    writeToFile (shuffleStartEndContent, new File (dataDir, "shuffleStartEnd.txt"))
    writeToFile (shuffleDurationContent, new File (dataDir, "shuffleDurationLO.txt"))
    writeToFile (verticesContent, new File (dataDir, "vertexOrder.txt"))
    writeToFile (listOfTasksContent, new File (dataDir, "vertexLtask.txt"))
    writeToFile (taskNodesContent, new File (dataDir, "taskNode.txt"))
    writeToFile (taskContainersContent, new File (dataDir, "taskContainer.txt"))
    writeToFile (shuffleBytesContent, new File (dataDir, "shuffleBytes.txt"))
  }

  private def writeToFile (content: String, file: File): Unit = {
    if (! content.isEmpty) {
      file.delete
      val out = new FileWriter (file)
      out write {
        if (content.last == '\n') content
        else content :+ '\n'
      }
      out.flush()
      out.close()
    }
  }

  private def copyFile (from: File, to: File): Unit = {
    val content = Source.fromFile (from).mkString
    to.delete
    val copy = new FileWriter(to)
    copy.write(content)
    copy.flush()
    copy.close()
  }

  private def parseOpts (args: Array[String]) = {
    val path = args(1)
    val firstArgument = args(0) match {
      case "-2" => Some(HiveTez_HDP22)
      case "-3" => Some(HiveTez_HDP23)
      case _ => None
    }
    firstArgument match {
      case Some(regexVersion) => Parser.parse(path, regexVersion)
      case None =>
        Console.err println WRONG_INPUT
        Console.err println USAGE
        System exit 1
    }
  }

  def main(args: Array[String]): Unit = if (args.length == 2) Parser parseOpts args else println(USAGE)

}
