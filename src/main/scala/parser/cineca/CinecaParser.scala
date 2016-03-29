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

import java.io.File

import parser.Parser

import scala.sys.process.Process

object CinecaParser {

  def processSeries(series : String) = {
    val mainInput = CinecaSettings.inputDir + "/" + series;
    val directory = new File(mainInput);
    val files = directory.listFiles().map(_.getName).toSeq;
    val mainOutput = CinecaSettings.outputDir + "/" + series;
    //if(!new File(mainOutput).exists()) {
    files.foreach(file => Parser.parse(mainInput + "/" + file, mainOutput + "/" + file, HiveTez_Cineca));
    val result = Process(Seq("rsync", "-r", "--chmod=D775,F664", mainOutput, CinecaSettings.remoteHost)).!;
    if(result != 0){
      throw new Exception("Something went wrong!");
    }
    //}
  }

  def main(args: Array[String]) {
    val sources = new File(CinecaSettings.inputDir).list();
    val inputs = if(sources != null) sources.toSeq; else Seq();
    inputs.foreach(input => processSeries(input));
  }

}
