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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * Generates synthetic plis with given cluster size and number of clusters.
 *
 * @author Jakob Zwiener
 */
public class PLIGenerator {

  protected static Random rand = new Random(42);

  public static void main(String[] args) throws IOException {

    FileOutputStream fout = new FileOutputStream("2m4-gen.plis");
    ObjectOutputStream oos = new ObjectOutputStream(fout);

    for (int i = 0; i < 100; i++) {
      oos.writeObject(generatePli(2 * 1000 * 1000, 4));
    }
  }

  public static PositionListIndex generatePli(long clusterSize, int numberOfClusters) {
    List<LongArrayList> clusters = new LinkedList<>();

    long range = clusterSize * numberOfClusters;

    for (int i = 0; i < numberOfClusters; i++) {
      LongArrayList cluster = new LongArrayList((int) clusterSize);
      for (long j = 0; j < clusterSize; j++) {

        cluster.add((long) (rand.nextDouble() * range));
      }
      clusters.add(cluster);
    }

    return new PositionListIndex(clusters);
  }

}
