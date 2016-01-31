package parser

object HiveTez_HDP22 extends StatusRegex {

  override val init = """(?<=LogType:syslog_)attempt_\d+_\d+_\d+_\d+_\d+_\d+""".r
  override val date = """\d+-\d+-\d+ \d+:\d+:\d+,\d+""".r
  override val vertex = """Routing pending task events for vertex: vertex_\d+_\d+_\d+_\d+ \[(.+)\]""".r
  override val taskToVertex = """impl.TaskAttemptImpl: remoteTaskSpec:DAGName.+VertexName: (.+), VertexParallelism.+TaskAttemptID:(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""".r("vertex", "task")
  override val taskToContainer = """Assigned taskAttempt.+(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+).+to container:.+(container_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""".r("task", "container")
  override val receivedContainer = """Assigning container to task, container=Container: \[ContainerId: (container_[0-9]+_[0-9]+_[0-9]+_[0-9]+).* containerHost=(\w+)""".r("container", "node")
  override val startingShuffle = """orderedgrouped.Shuffle: Shuffle assigned with""".r
  override val endingShuffle = """orderedgrouped.Shuffle: Shutting down Shuffle for source""".r
  override val shuffleBytes = """Final Counters :.*SHUFFLE_BYTES=([0-9]+),""".r("bytes", "task")

}
