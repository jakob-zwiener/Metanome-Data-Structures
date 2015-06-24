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

import java.util.concurrent.Executors;

import de.metanome.algorithm_helper.data_structures.PositionListIndex;

/**
 * @author Jakob Zwiener
 */
public class PLISyntheticBenchmark {

  public static void main(String[] args) {

    for (int threads = 1; threads <= 4; threads++) {
      PositionListIndex.exec = Executors.newFixedThreadPool(threads);
      for (int iteration = 0; iteration < 2; iteration++) {
        PositionListIndex left = PLIGenerator.generatePli(16 * 1000000, 8);
        PositionListIndex right = PLIGenerator.generatePli(16 * 1000000, 8);
        long beforeIntersect = System.nanoTime();
        left.intersect(right);
        long afterIntersect = System.nanoTime();
        System.out.println(
          String.format("intersected in %fs.", (afterIntersect - beforeIntersect) / 1000000000d));
      }
      PositionListIndex.exec.shutdown();
    }


  }

}
