/* Copyright 2015-2016 Eugenio Gianniti
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

object HiveTez_HDP23 extends StatusRegex {

  override val init = """(?<=LogType:syslog_)attempt_\d+_\d+_\d+_\d+_\d+_\d+""".r
  override val date = """\d+-\d+-\d+ \d+:\d+:\d+,\d+""".r
  override val vertex = """Routing pending task events for vertex: vertex_\d+_\d+_\d+_\d+ \[(.+)\]""".r
  override val taskToVertex = """\[Event:TASK_ATTEMPT_STARTED\]: vertexName=(.+), taskAttemptId=(attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""".r("vertex", "task")
  override val taskToContainer = """Container with id: (container_.+_[0-9]+_[0-9]+_[0-9]+_[0-9]+) given task: (attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+)""".r("container", "task")
  override val receivedContainer = """Assigning container to task: containerId=(container_.+_[0-9]+_[0-9]+_[0-9]+_[0-9]+), task=.* containerHost=(\w+)""".r("container", "node")
  override val startingShuffle = """Shuffle assigned with""".r
  override val endingShuffle = """Shutting down Shuffle for source""".r
  override val shuffleBytes = """Final Counters for (attempt_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+):.*SHUFFLE_BYTES=([0-9]+),""".r("task", "bytes")

}
