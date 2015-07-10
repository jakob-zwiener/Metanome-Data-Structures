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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_helper.data_structures.PLIBuildingException;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;

/**
 * Benchmark to evaluate multi pli intersects.
 * @author Jakob Zwiener
 */
public class PLIMultiIntersectBenchmark {

  public static void main(String[] args)
    throws AlgorithmConfigurationException, PLIBuildingException, ClassNotFoundException, IOException
  {
    List<PositionListIndex> plis = PLIBenchmarkRunner.getPlis("ncvoter.plis", "ncvoter.csv");

    Map<ColumnCombinationBitset, PositionListIndex> pliStore = new HashMap<>();
    for (int i = 0; i < plis.size(); i++) {
      pliStore.put(new ColumnCombinationBitset(i), plis.get(i));
    }

    BufferedReader input = new BufferedReader(new FileReader("ncvoter20col_multiPliIntersect.txt"));

    long startPLIIntersects = System.nanoTime();

    while (true) {
      String line = input.readLine();

      if (line == null) {
        break;
      }

      String[] columnCombinationRepresentations = line.split(";");

      ColumnCombinationBitset
        left =
        ColumnCombinationBitset.fromString(columnCombinationRepresentations[1]);
      ColumnCombinationBitset right = ColumnCombinationBitset.fromString(
        columnCombinationRepresentations[2]);

      System.out.println("" + pliStore.containsKey(left) + pliStore.containsKey(right));

      List<PositionListIndex> rightColumns = new ArrayList<>();
      for (ColumnCombinationBitset oneColumn : right.getContainedOneColumnCombinations()) {
        rightColumns.add(pliStore.get(oneColumn));
      }

      if (!pliStore.containsKey(left)) {
        PositionListIndex intersectedLeft = null;
        for (ColumnCombinationBitset oneColumnCombination : left.getContainedOneColumnCombinations()) {
          // FIXME(zwiener): baseline should store intermediate plis?
          if (intersectedLeft == null) {
            intersectedLeft = pliStore.get(oneColumnCombination);
            continue;
          }

          intersectedLeft = intersectedLeft.intersect(pliStore.get(oneColumnCombination));
        }
        pliStore.put(left, intersectedLeft);
      }

      PositionListIndex intersectedPLI = pliStore.get(left).intersect(rightColumns.toArray(
        new PositionListIndex[rightColumns.size()]));

      pliStore.put(left.union(right), intersectedPLI);

      System.out.println(left.toString() + right.toString());

    }

    long endPLIIntersects = System.nanoTime();

    System.out.println(String.format("Intersects took %fs.", (endPLIIntersects - startPLIIntersects) / 1000000000d));
  }

}
