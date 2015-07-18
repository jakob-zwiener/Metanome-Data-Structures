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
import java.util.List;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_helper.data_structures.PLIBuildingException;
import de.metanome.algorithm_helper.data_structures.PLIManager;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;

/**
 * TODO(zwiener): docs
 * @author Jakob Zwiener
 */
public class PLIMultiIntersectBenchmarkRealData {

  public static void main(String[] args)
    throws AlgorithmConfigurationException, PLIBuildingException, ClassNotFoundException, IOException
  {
    List<PositionListIndex> plis = PLIBenchmarkRunner.getPlis("ncvoter.plis", "ncvoter.csv");

    PLIManager pliManager = new PLIManager(plis);

    /*int j = 0;
    for (PositionListIndex pli : plis) {
      System.out.println(j++);
      System.out.println(pli.getRawKeyError());
      for (IntArrayList cluster : pli.getClusters()) {
        int i = 0;
        for (int rowIndex : cluster) {
          if (rowIndex > pli.getNumberOfRows()) {
            System.out.println("failure");
            System.out.println(i);
          }
          i++;
        }
      }
    }*/

    int[] allColumnIndices = new int[plis.size()];
    for (int i = 0; i < plis.size(); i++) {
      allColumnIndices[i] = i;
    }
    ColumnCombinationBitset allColumnCombination = new ColumnCombinationBitset(allColumnIndices);

    allColumnCombination.removeColumn(91);

    long start = System.nanoTime();
    pliManager.buildPli(allColumnCombination);
    System.out.println((System.nanoTime() - start) / 1000000000d);

    PLIManager.exec.shutdown();
  }

}
