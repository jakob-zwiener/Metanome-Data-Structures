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

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_helper.data_structures.PLIBuilder;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.backend.input.file.DefaultFileInputGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jakob Zwiener
 */
public class PLIBenchmarkRunner {

  public static void main(String[] args)
      throws IOException, InputGenerationException, InputIterationException {

    long beforePLIBuild = System.nanoTime();

    PLIBuilder
        pliBuilder =
        new PLIBuilder(new DefaultFileInputGenerator(new File("uniprot.csv")).generateNewCopy());

    List<PositionListIndex> plis = pliBuilder.getPLIList();

    long afterPLIBuild = System.nanoTime();

    System.out.printf("PLI build took: %fs\n", (afterPLIBuild - beforePLIBuild) / 1000000000d);

    Map<ColumnCombinationBitset, PositionListIndex> pliStore = new HashMap<>();
    for (int i = 0; i < plis.size(); i++) {
      pliStore.put(new ColumnCombinationBitset(i), plis.get(i));
    }

    BufferedReader input = new BufferedReader(new FileReader("uniprot_incomplete.csv"));

    long numberOfIntersects = 0;
    long secondsOfBenchmark = 30;
    long startPLIIntersects = System.nanoTime();

    while (true) {
      String line = input.readLine();
      if (line == null) {
        break;
      }

      if (System.nanoTime() - startPLIIntersects >= secondsOfBenchmark * 1000000000) {
        break;
      }

      String[] columnCombinationRepresentations = line.split(";");

      ColumnCombinationBitset
          left =
          ColumnCombinationBitset.fromString(columnCombinationRepresentations[0]);
      ColumnCombinationBitset right = ColumnCombinationBitset.fromString(
          columnCombinationRepresentations[1]);

      pliStore.put(left.union(right), pliStore.get(left).intersect(pliStore.get(right)));
      numberOfIntersects++;
    }

    long afterParsing = System.nanoTime();

    System.out
        .printf("%f intersections/s over %ds.\n", numberOfIntersects / (double) secondsOfBenchmark,
                secondsOfBenchmark);


  }


}
