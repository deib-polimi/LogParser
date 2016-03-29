/* Copyright 2016 Alessandro Maria Rizzi
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
package parser.cineca

import parser.StatusRegex

object HiveTez_Cineca extends StatusRegex{
  override val init = """attempt_\d+_\d+_\d+_\d+_\d+_\d+""".r//TODO: FixMe!!! (?<=LogType:syslog_)
  override val date = """\d+-\d+-\d+ \d+:\d+:\d+,\d+""".r
  override val vertex = """Routing pending task events for vertex: vertex_\d+_\d+_\d+_\d+ \[(.+)\]""".r
  override val taskToVertex = """impl.TaskAttemptImpl: remoteTaskSpec:DAGName.+VertexName: (.+), VertexParallelism.+TaskAttemptID:(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""".r("vertex", "task")
  override val taskToContainer = """Assigned taskAttempt.+(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+).+to container:.+(container_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""".r("task", "container")
  override val receivedContainer = """Assigning container to task, container=Container: \[ContainerId: (container_[0-9]+_[0-9]+_[0-9]+_[0-9]+).* containerHost=(\w+)""".r("container", "node")
  override val startingShuffle = """orderedgrouped.Shuffle: Shuffle assigned with""".r
  override val endingShuffle = """orderedgrouped.Shuffle: Shutting down Shuffle for source""".r
  override val shuffleBytes = """Final Counters :.*SHUFFLE_BYTES=([0-9]+),""".r("bytes", "task")
}
