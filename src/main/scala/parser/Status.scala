/**
  *
  */
package parser

import java.text.SimpleDateFormat
import java.util.Locale

/**
  * @author Alessandro
  *
  */
case class Waiting (tm : Map[String, (Long, Long)], vs : Seq[String],
                    tv : Map[String, Seq[String]], tc : Map[String, String],
                    to : Seq[String], cn : Map[String, String],
                    stm : Map[String, (Long, Long)], sb : Map[String, Long], regex : StatusRegex)
  extends Status (tm, vs, tv, tc, to, cn, stm, sb, regex)

class Start (statusRegex : StatusRegex)
  extends Waiting (Map(), Seq(), Map(), Map(), Seq(), Map(), Map(), Map(), statusRegex)

object Start {
  def apply (statusRegex: StatusRegex): Start = new Start(statusRegex)
}

case class Init (name : String, tm : Map[String, (Long, Long)], vs : Seq[String],
                 tv : Map[String, Seq[String]], tc : Map[String, String],
                 to : Seq[String], cn : Map[String, String],
                 stm : Map[String, (Long, Long)], sb : Map[String, Long], regex : StatusRegex)
  extends Status (tm, vs, tv, tc, to, cn, stm, sb, regex)

case class Started (name : String, startTime : Long, endTime : Long,
                    tm : Map[String, (Long, Long)], vs : Seq[String],
                    tv : Map[String, Seq[String]], tc : Map[String, String],
                    to : Seq[String], cn : Map[String, String],
                    stm : Map[String, (Long, Long)], sb : Map[String, Long],
                    regex : StatusRegex)
  extends Status (tm, vs, tv, tc, to, cn, stm, sb, regex)

case class Shuffling (name : String, startTask : Long, endTask : Long,
                      startShuffle : Long, tm : Map[String, (Long, Long)],
                      vs : Seq[String], tv : Map[String, Seq[String]],
                      tc : Map[String, String], to : Seq[String],
                      cn : Map[String, String], stm : Map[String, (Long, Long)],
                      sb : Map[String, Long], regex : StatusRegex)
  extends Status (tm, vs, tv, tc, to, cn, stm, sb, regex)

sealed abstract class Status (tm : Map[String, (Long, Long)], vs : Seq[String],
                              tv : Map[String, Seq[String]], tc : Map[String, String],
                              to : Seq[String], cn : Map[String, String],
                              stm : Map[String, (Long, Long)], sb : Map[String, Long],
                              statusRegex : StatusRegex) {

  def times: Map[String, (Long, Long)] = tm
  def vertices: Seq[String] = vs
  def taskToVertices: Map[String, Seq[String]] = tv
  def taskToContainers: Map[String, String] = tc
  def taskOrder: Seq[String] = to
  def containerToNodes: Map[String, String] = cn
  def shuffleTimes: Map[String, (Long, Long)] = stm
  def shuffleBytes: Map[String, Long] = sb

  protected def parseTime(input : String) =
    new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss,SSS", Locale.ENGLISH).parse (input).getTime

  def next (line : String) : Status = {

    def nextVertices(): Seq[String] = statusRegex.vertex findFirstMatchIn line match {
      case Some (m) => vertices :+ (m group 1)
      case None => vertices
    }

    def nextTaskToVertices(): Map[String, Seq[String]] =
      statusRegex.taskToVertex findFirstMatchIn line match {
        case Some (m) =>
          val (vertex, task) = (m group "vertex", m group "task")
          if (taskToVertices contains vertex) {
            val nextSeq = taskToVertices (vertex) :+ task
            taskToVertices + (vertex -> nextSeq)
          }
          else {
            val nextSeq = Seq (task)
            taskToVertices + (vertex -> nextSeq)
          }
        case None => taskToVertices
      }

    def nextTaskToContainers(): Map[String, String] =
      statusRegex.taskToContainer findFirstMatchIn line match {
        case Some (m) =>
          val (task, container) = (m group "task", m group "container")
          taskToContainers + (task -> container)
        case None => taskToContainers
      }

    def nextTaskOrder(): Seq[String] =
      statusRegex.taskToContainer findFirstMatchIn line match {
        case Some (m) => taskOrder :+ (m group "task")
        case None => taskOrder
      }

    def nextContainerToNodes(): Map[String, String] =
      statusRegex.receivedContainer findFirstMatchIn line match {
        case Some (m) =>
          val (container, node) = (m group "container", m group "node")
          containerToNodes + (container -> node)
        case None => containerToNodes
      }

    def nextShuffleBytes(taskName: String): Map[String, Long] =
      statusRegex.shuffleBytes findFirstMatchIn line match {
        case Some (m) => val bytes = {m group "bytes"}.toLong
          val task = try {m group "task"}
            catch {case e: ArrayIndexOutOfBoundsException => taskName}
          shuffleBytes + (task -> bytes)
        case None => shuffleBytes
      }

    this match {
      case Waiting (_, _, _, _, _, _, _, _, _) =>
        val name = statusRegex.init findFirstIn line
        if (name.isDefined) Init (name.get, times, nextVertices(), nextTaskToVertices(),
          nextTaskToContainers(), nextTaskOrder(),
          nextContainerToNodes(), shuffleTimes, shuffleBytes, statusRegex)
        else Waiting (times, nextVertices(), nextTaskToVertices(),
          nextTaskToContainers(), nextTaskOrder(),
          nextContainerToNodes(), shuffleTimes, shuffleBytes, statusRegex)

      case Init (name, _, _, _, _, _, _, _, _, _) =>
        val when = statusRegex.date findFirstIn line
        if (when.isDefined) {
          val time = parseTime (when.get)
          Started (name, time, time, times, nextVertices(), nextTaskToVertices(),
            nextTaskToContainers(), nextTaskOrder(),
            nextContainerToNodes(), shuffleTimes, nextShuffleBytes(name), statusRegex)
        }
        else Init (name, times, nextVertices(), nextTaskToVertices(),
          nextTaskToContainers(), nextTaskOrder(),
          nextContainerToNodes(), shuffleTimes, nextShuffleBytes(name), statusRegex)

      case Started (name, start, end, _, _, _, _, _, _, _, _, _) =>
        def lookForShuffle = {
          def updateTime() = {
            val time = statusRegex.date findFirstIn line
            if (time.isDefined) Started (name, start, parseTime (time.get),
              times, nextVertices(), nextTaskToVertices(),
              nextTaskToContainers(), nextTaskOrder(),
              nextContainerToNodes(), shuffleTimes, nextShuffleBytes(name), statusRegex)
            else Started (name, start, end, times, nextVertices(),
              nextTaskToVertices(), nextTaskToContainers(),
              nextTaskOrder(), nextContainerToNodes(), shuffleTimes,
              nextShuffleBytes(name), statusRegex)
          }
          statusRegex.startingShuffle findFirstMatchIn line match {
            case Some (_) =>
              val time = parseTime ((statusRegex.date findFirstIn line).get)
              Shuffling (name, start, time, time, times, nextVertices(),
                nextTaskToVertices(), nextTaskToContainers(),
                nextTaskOrder(), nextContainerToNodes(), shuffleTimes,
                nextShuffleBytes(name), statusRegex)
            case None => updateTime()
          }
        }
        if (line.isEmpty) Waiting (times + (name -> (start, end)), nextVertices(),
          nextTaskToVertices(), nextTaskToContainers(),
          nextTaskOrder(), nextContainerToNodes(),
          shuffleTimes, nextShuffleBytes(name), statusRegex)
        else lookForShuffle

      case Shuffling (name, startTask, endTask, startShuffle, _, _, _, _, _, _, _, _, _) =>
        def updateTime() = {
          val time = statusRegex.date findFirstIn line
          if (time.isDefined) Shuffling (name, startTask, parseTime (time.get),
            startShuffle, times, nextVertices(),
            nextTaskToVertices(), nextTaskToContainers(),
            nextTaskOrder(), nextContainerToNodes(),
            shuffleTimes, nextShuffleBytes(name), statusRegex)
          else Shuffling (name, startTask, endTask, startShuffle, times,
            nextVertices(), nextTaskToVertices(), nextTaskToContainers(),
            nextTaskOrder(), nextContainerToNodes(), shuffleTimes,
            nextShuffleBytes(name), statusRegex)
        }
        statusRegex.endingShuffle findFirstMatchIn line match {
          case Some (_) =>
            val time = parseTime ((statusRegex.date findFirstIn line).get)
            Started (name, startTask, time, times, nextVertices(),
              nextTaskToVertices(), nextTaskToContainers(), nextTaskOrder(),
              nextContainerToNodes(),
              shuffleTimes + (name -> (startShuffle, time)),
              nextShuffleBytes(name), statusRegex)
          case None => updateTime()
        }
    }
  }
}
