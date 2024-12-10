/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.highlights;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;

import java.util.*;

public class TopPhraseOnlyBaseFragmentsBuilderAdaptor extends BaseFragmentsBuilder {
  private final BaseFragmentsBuilder innerBaseFragmentsBuilder;
  private final boolean oncePerTerm;
  private final boolean highestBoostOnly;

  /** a constructor. */
  public TopPhraseOnlyBaseFragmentsBuilderAdaptor(BaseFragmentsBuilder baseFragmentsBuilder) {
    this(baseFragmentsBuilder, false, false);
  }

  public TopPhraseOnlyBaseFragmentsBuilderAdaptor(BaseFragmentsBuilder baseFragmentsBuilder, boolean oncePerTerm, boolean highestBoostOnly) {
    super();
    this.innerBaseFragmentsBuilder = baseFragmentsBuilder;
    this.oncePerTerm = oncePerTerm;
    this.highestBoostOnly = highestBoostOnly;
  }


  @Override
  public List<WeightedFragInfo> getWeightedFragInfoList(List<WeightedFragInfo> src) {
    return innerBaseFragmentsBuilder.getWeightedFragInfoList(src);
  }

  @Override
  protected String makeFragment(
      StringBuilder buffer,
      int[] index,
      Field[] values,
      WeightedFragInfo fragInfo,
      String[] preTags,
      String[] postTags,
      Encoder encoder) {
    StringBuilder fragment = new StringBuilder();
    final int s = fragInfo.getStartOffset();
    int[] modifiedStartOffset = {s};
    String src =
        getFragmentSourceMSO(
            buffer, index, values, s, fragInfo.getEndOffset(), modifiedStartOffset);
    int srcIndex = 0;
    List<SubInfo> subInfos;
    if (this.highestBoostOnly || this.oncePerTerm) {
      float maxBoost = 0f;
      subInfos = new ArrayList<>();
      Set<String> termSet = new HashSet<>();
      for (SubInfo subInfo : fragInfo.getSubInfos()) {
        if ((!termSet.add(subInfo.getText()) && this.oncePerTerm)) {
          continue;
        }
        if (this.highestBoostOnly) {
          if (subInfo.getBoost() > maxBoost) {
            maxBoost = subInfo.getBoost();
            subInfos.clear();
          } else if (subInfo.getBoost() < maxBoost) {
            continue;
          }
        }
        subInfos.add(subInfo);
      }
    }else {
      subInfos = fragInfo.getSubInfos();
    }

    for (SubInfo subInfo : subInfos) {
      for (Toffs to : subInfo.getTermsOffsets()) {
        fragment
            .append(
                encoder.encodeText(
                    src.substring(srcIndex, to.getStartOffset() - modifiedStartOffset[0])))
            .append(getPreTag(preTags, subInfo.getSeqnum()))
            .append(
                encoder.encodeText(
                    src.substring(
                        to.getStartOffset() - modifiedStartOffset[0],
                        to.getEndOffset() - modifiedStartOffset[0])))
            .append(getPostTag(postTags, subInfo.getSeqnum()));
        srcIndex = to.getEndOffset() - modifiedStartOffset[0];
      }
    }
    fragment.append(encoder.encodeText(src.substring(srcIndex)));
    return fragment.toString();
  }
}
