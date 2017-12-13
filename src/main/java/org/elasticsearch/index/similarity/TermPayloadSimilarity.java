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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.Loggers;

public class TermPayloadSimilarity extends Similarity {

  @Override
  public long computeNorm(FieldInvertState state) {
    return 1;
  }

  @Override
  public SimWeight computeWeight(float boost, CollectionStatistics collectionStats,
      TermStatistics... termStats) {
    return new TermPayloadWeight(boost, collectionStats, termStats);
  }

  @Override
  public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
    TermPayloadWeight termPayloadWeight = (TermPayloadWeight) weight;
    return new TermPayloadScorer(termPayloadWeight, context);
  }

  private static class TermPayloadWeight extends SimWeight {

    private float boost;
    private final String field;
    private final TermStatistics[] termStats;

    TermPayloadWeight(float boost, CollectionStatistics collectionStats,
        TermStatistics... termStats) {
      this.boost = boost;
      this.field = collectionStats.field();
      this.termStats = termStats;
    }
  }

  private final class TermPayloadScorer extends SimScorer {

    private final TermPayloadWeight weight;
    private final LeafReaderContext context;
    private final List<Explanation> explanations = new ArrayList<>();

    TermPayloadScorer(TermPayloadWeight weight, LeafReaderContext context) throws IOException {
      this.weight = weight;
      this.context = context;
    }

    /**
     * @param doc Document id.
     * @param freq A term frequency. This function ignores it.
     * @return A term score for a payload field in a document. Depends on a weight of the term.
     */
    public float score(int doc, float freq) {
      float totalScore = 0.0f;
      int i = 0;
      int length = weight.termStats.length;

      while (i < length) {

//        try {
//          String term = new String(weight.termStats[i].term().bytes, "UTF-8");
//
//          Loggers.getLogger(this.getClass())
//              .info("length:" + length + ", [" + doc + "]termStats : " + term);
//        } catch (Exception e) {
//
//        }

        totalScore += scoreTerm(doc, weight.termStats[i].term());
        i++;
      }
      return totalScore;
    }

    private float scoreTerm(int doc, BytesRef term) {
      float termWeight = termPayloadWeight(doc, term);
      float termScore = weight.boost * termWeight;
      String func = weight.boost + "*" + termWeight;    // You can change scoreTerm equation.
      explanations.add(
          Explanation.match(
              termScore,
              "score(boost=" + weight.boost + ", termScore=" + termScore + ", termWeight="
                  + termWeight + ", func=" + func + ")"
          )
      );

      return termScore;
    }

    private float termPayloadWeight(int doc, BytesRef term) {
      int defaultWeight = 1;

      try {
        Terms terms = context.reader().getTermVector(doc, weight.field);
        TermsEnum termsEnum = terms.iterator();

        if (!termsEnum.seekExact(term)) {
          Loggers
              .getLogger(this.getClass())
              .error("seekExact failed, returning default term weight = " +
                  defaultWeight + " for field = " + weight.field);

          return defaultWeight;
        }

        PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
        dpEnum.nextDoc();
        dpEnum.nextPosition();
        BytesRef payload = dpEnum.getPayload();

        if (payload == null) {
          Loggers.getLogger(this.getClass())
              .error("getPayload failed, returning default term weight = " +
                  defaultWeight + " for field = " + weight.field);

          return defaultWeight;
        }

        return PayloadHelper.decodeFloat(payload.bytes, payload.offset);

      } catch (Exception ex) {
        Loggers.getLogger(this.getClass())
            .error("Unexpected exception, returning default term weight = " +
                defaultWeight + " for field = " + weight.field, ex);

        return defaultWeight;
      }
    }

    public float computeSlopFactor(int distance) {
      return 1.0f / (distance + 1);
    }

    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return 1.0f;
    }

    public Explanation explain(int doc, Explanation freq) {
      return Explanation.match(
          score(doc, freq.getValue()),
          "term payload score(doc=" + doc + ", freq=" + freq.getValue() + "), sum of:",
          explanations
      );
    }
  }
}
