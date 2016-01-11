package parser

import scala.util.matching.Regex

object HiveTez_HDP22 extends StatusRegex {

  override val init = """(?<=LogType:syslog_)attempt_\d+_\d+_\d+_\d+_\d+_\d+""".r
  override val date = """\d+-\d+-\d+ \d+:\d+:\d+,\d+""".r
  override val vertex = """Routing pending task events for vertex: vertex_\d+_\d+_\d+_\d+ \[(.+)\]""".r
  override val taskToVertex = new Regex ("""impl.TaskAttemptImpl: remoteTaskSpec:DAGName.+VertexName: (.+), VertexParallelism.+TaskAttemptID:(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""", "vertex", "task")
  override val taskToContainer = new Regex ("""Assigned taskAttempt.+(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+).+to container:.+(container_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""", "task", "container")
  override val receivedContainer = new Regex ("""Assigning container to task, container=Container: \[ContainerId: (container_[0-9]+_[0-9]+_[0-9]+_[0-9]+).* containerHost=(\w+)""", "container", "node")
  override val startingShuffle = """orderedgrouped.Shuffle: Shuffle assigned with""".r
  override val endingShuffle = """orderedgrouped.Shuffle: Shutting down Shuffle for source""".r
  override val shuffleBytes = new Regex ("""Final Counters :.*SHUFFLE_BYTES=([0-9]+),""", "bytes", "task")

}
