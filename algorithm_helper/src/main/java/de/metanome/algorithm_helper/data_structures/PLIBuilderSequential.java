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

package de.metanome.algorithm_helper.data_structures;

import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.backend.input.file.InvalidMaskException;
import de.metanome.backend.input.file.MaskingRelationalInput;

import java.util.ArrayList;
import java.util.List;

/**
 * Constructs a list of {@link PositionListIndex}es from the given {@link
 * de.metanome.algorithm_integration.input.RelationalInputGenerator}. The plis are built
 * sequentially.
 *
 * @author Jakob Zwiener
 * @see GenericPLIBuilder
 * @see PLIBuilder
 */
public class PLIBuilderSequential implements GenericPLIBuilder {

  protected final RelationalInputGenerator inputGenerator;
  protected boolean nullEqualsNull;

  public PLIBuilderSequential(RelationalInputGenerator inputGenerator) {
    this.inputGenerator = inputGenerator;
    this.nullEqualsNull = true;
  }

  public PLIBuilderSequential(RelationalInputGenerator inputGenerator, boolean nullEqualsNull) {
    this(inputGenerator);
    this.nullEqualsNull = nullEqualsNull;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<PositionListIndex> getPLIList() throws PLIBuildingException {
    List<PositionListIndex> pliList = new ArrayList<>();

    int numberOfColumns;
    try {
      numberOfColumns = inputGenerator.generateNewCopy().numberOfColumns();
    } catch (InputGenerationException e) {
      throw new PLIBuildingException(
          "The pli could not be built, because there was an error generating the input.", e);
    }

    for (int i = 0; i < numberOfColumns; i++) {
      MaskingRelationalInput
          maskingInput;
      try {
        maskingInput = new MaskingRelationalInput(inputGenerator.generateNewCopy(), i);
      } catch (InvalidMaskException e) {
        throw new PLIBuildingException(String.format(
            "The pli could not be build, because the given mask was invalid. Column to mask was: %d.",
            i), e);
      } catch (InputGenerationException e) {
        throw new PLIBuildingException(
            "The pli could not be built, because there was an error generating the input.", e);
      }
      pliList.add(new PLIBuilder(maskingInput, nullEqualsNull).getPLIList().get(0));
    }

    return pliList;
  }

}
