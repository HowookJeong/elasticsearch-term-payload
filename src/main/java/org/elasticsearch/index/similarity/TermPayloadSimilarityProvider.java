/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.similarity;

import org.elasticsearch.common.settings.Settings;

public class TermPayloadSimilarityProvider extends AbstractSimilarityProvider {
  private final TermPayloadSimilarity similarity;

  public TermPayloadSimilarityProvider(String name, Settings settings, Settings indexSettings) {
    super(name);
    this.similarity = new TermPayloadSimilarity();
  }

  public TermPayloadSimilarity get() {
    return similarity;
  }
}
