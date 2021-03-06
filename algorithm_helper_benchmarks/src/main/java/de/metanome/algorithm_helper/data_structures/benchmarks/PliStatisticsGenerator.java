/*
 * Copyright 2015 by the Metanome project
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

package de.metanome.algorithm_helper.data_structures.benchmarks;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import de.metanome.algorithm_helper.data_structures.PLIBuildingException;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * @author Jakob Zwiener
 */
public class PliStatisticsGenerator {

  public static void main(String[] args)
      throws PLIBuildingException, IOException, AlgorithmConfigurationException,
             ClassNotFoundException {

    List<PositionListIndex> plis = PLIBenchmarkRunner.getPlis("ncvoter.plis", "ncvoter.csv");

    PrintWriter statisticsOutput = new PrintWriter("statistics.txt");
    for (PositionListIndex pli : plis) {
      List<IntArrayList> clusters = pli.getClusters();
      StringBuilder lineStatistics = new StringBuilder();
      lineStatistics.append(clusters.size()).append(": ");
      for (IntArrayList cluster : clusters) {
        lineStatistics.append(cluster.size()).append(", ");
      }
      statisticsOutput.println(lineStatistics.toString());
      System.out.println(lineStatistics.toString());
    }


  }

}
