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

import de.metanome.algorithm_helper.data_structures.PLIBuildingException;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.Executors;

/**
 * @author Jakob Zwiener
 */
public class PLISyntheticBenchmark {

  public static void main(String[] args) throws IOException, PLIBuildingException {
    final int iterations = 3;
    final int[] threadNumbers = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    final int[] clusterNumbers = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    final int[] clusterSizes = {
      125 * 1000,  // 125k,
      250 * 1000,  // 250k,
      5 * 100 * 1000,  // 500k,
      1 * 1000 * 1000,  // 1m,
      2 * 1000 * 1000,  // 2m,
      4 * 1000 * 1000,  // 4m,
      8 * 1000 * 1000,  // 8m,
    };

    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("benchmark_result.csv"), "utf-8"))) {
      writer.write(Joiner.on(',').join("iteration", "numberOfClusters", "clusterSize", "threads", "timeElapsedS"));
      writer.newLine();

      for (int threads : threadNumbers) {
        PositionListIndex.exec = Executors.newFixedThreadPool(threads);
        for (int numberOfClusters : clusterNumbers) {
          for (int clusterSize : clusterSizes) {
            for (int iteration = 0; iteration < iterations; iteration++) {
              PositionListIndex left = PLIGenerator.generatePli(clusterSize, numberOfClusters);
              PositionListIndex right = PLIGenerator.generatePli(clusterSize, numberOfClusters);
              long beforeIntersect = System.nanoTime();
              left.intersect(right);
              long afterIntersect = System.nanoTime();
              final double timeElapsedS = (afterIntersect - beforeIntersect) / 1000000000d;
              System.out.println(
                String.format("Intersect iteration %d with %d clusters of size %d computed with %d threads in %fs.",
                  iteration, numberOfClusters, clusterSize, threads, timeElapsedS));
              writer.write(Joiner.on(',').join(iteration, numberOfClusters, clusterSize, threads, timeElapsedS));
              writer.newLine();
              writer.flush();
              System.gc();
            }
          }
        }
        PositionListIndex.exec.shutdown();
      }
    }
  }
}
