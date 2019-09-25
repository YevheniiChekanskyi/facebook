/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.facebook.source.streaming;

import com.facebook.ads.sdk.APIException;
import com.facebook.ads.sdk.APINodeList;
import com.facebook.ads.sdk.AdsInsights;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.facebook.source.common.AdsInsightsTransformer;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequest;
import io.cdap.plugin.facebook.source.common.requests.InsightsRequestFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.Option;
import scala.reflect.ClassTag;

import java.util.LinkedList;
import java.util.List;

/**
 * Iterates over Facebook Insights api response and fills Spark RDD with structured records from it.
 */
public class FacebookInputDStream extends InputDStream<StructuredRecord> {
  private final FacebookStreamingSourceConfig config;

  public FacebookInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> evidence1,
                              FacebookStreamingSourceConfig config) {
    super(ssc, evidence1);
    this.config = config;
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time time) {
    try {
      return doCompute();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Option<RDD<StructuredRecord>> doCompute() throws APIException {
    List<StructuredRecord> records = new LinkedList<>();

    InsightsRequest request = InsightsRequestFactory.createRequest(config);
    APINodeList<AdsInsights> currentPage = request.execute();

    while (currentPage != null) {
      fetchPage(currentPage, records);
      currentPage = currentPage.nextPage();
    }

    RDD<StructuredRecord> rdds = getJavaSparkContext().parallelize(records).rdd();
    return Option.apply(rdds);
  }

  private void fetchPage(APINodeList<AdsInsights> page, List<StructuredRecord> records) {
    page.iterator().forEachRemaining(insight -> {
      records.add(AdsInsightsTransformer.transform(insight, config.getSchema()));
    });
  }

  private JavaSparkContext getJavaSparkContext() {
    return JavaSparkContext.fromSparkContext(ssc().sc());
  }
}
