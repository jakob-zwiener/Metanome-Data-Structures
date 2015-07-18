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

import com.google.common.base.Joiner;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_helper.data_structures.PLIBuildingException;
import de.metanome.algorithm_helper.data_structures.PLIManager;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO(zwiener): docs
 * @author Jakob Zwiener
 */
public class PLISyntheticMultiIntersectBenchmark {

  public static void main(String[] args) throws IOException, InterruptedException, PLIBuildingException {
    final int iterations = 3;
    final int[] clusterNumbers = { 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    final int[] clusterSizes = {
      /*125 * 1000,  // 125k,
      250 * 1000,  // 250k,
      5 * 100 * 1000,  // 500k,
      1 * 1000 * 1000,  // 1m,*/
      2 * 1000 * 1000,  // 2m,
      4 * 1000 * 1000,  // 4m,
      8 * 1000 * 1000,  // 8m,
    };
    final int numberOfPlis = 128;

    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("benchmark_result.csv"), "utf-8"))) {
      writer.write(Joiner.on(',').join("iteration", "numberOfClusters", "clusterSize", "threads", "timeElapsedS"));
      writer.newLine();

      // for (int threads : threadNumbers) {
      // PositionListIndex.exec = Executors.newFixedThreadPool(threads);
      for (int numberOfClusters : clusterNumbers) {
        for (int clusterSize : clusterSizes) {
          for (int iteration = 0; iteration < iterations; iteration++) {
            List<PositionListIndex> plis = new ArrayList<>(numberOfPlis);
            int[] columnIndices = new int[numberOfPlis];
            for (int i = 0; i < numberOfPlis; i++) {
              plis.add(PLIGenerator.generatePli(clusterSize, numberOfClusters));
              columnIndices[i] = i;
            }
            PLIManager pliManager = new PLIManager(plis);
            ColumnCombinationBitset allColumns = new ColumnCombinationBitset(columnIndices);

            long beforeIntersect = System.nanoTime();
            pliManager.buildPli(allColumns);
            long afterIntersect = System.nanoTime();
            final double timeElapsedS = (afterIntersect - beforeIntersect) / 1000000000d;
            System.out.println(
              String.format("Intersect iteration %d with %d clusters of size %d computed with %d threads in %fs.",
                iteration, numberOfClusters, clusterSize, 1, timeElapsedS));
            writer.write(Joiner.on(',').join(iteration, numberOfClusters, clusterSize, 1, timeElapsedS));
            writer.newLine();
            writer.flush();
            System.gc();
          }
        }
      }
      // PositionListIndex.exec.shutdown();
      // }
    }
  }
}
