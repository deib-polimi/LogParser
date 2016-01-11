package parser

/**
  * Created by eugenio on 11/01/16.
  */
case class ShuffleBytes(bytes: Map[String, Long]) {

  override def toString = bytes map {case (task, byteCount) => s"$task\t$byteCount"} mkString "\n"

}
