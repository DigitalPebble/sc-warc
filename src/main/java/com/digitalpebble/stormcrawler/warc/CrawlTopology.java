/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.warc;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;

import com.digitalpebble.storm.crawler.ConfigurableTopology;
import com.digitalpebble.storm.crawler.Constants;
import com.digitalpebble.storm.crawler.bolt.FetcherBolt;
import com.digitalpebble.storm.crawler.bolt.JSoupParserBolt;
import com.digitalpebble.storm.crawler.bolt.SiteMapParserBolt;
import com.digitalpebble.storm.crawler.bolt.URLPartitionerBolt;
import com.digitalpebble.storm.crawler.persistence.MemoryStatusUpdater;
import com.digitalpebble.storm.crawler.spout.MemorySpout;

import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Dummy topology to play with the spouts and bolts
 */
public class CrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new CrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MemorySpout("http://www.lemonde.fr"));

        builder.setBolt("partitioner", new URLPartitionerBolt())
                .shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt())
                .fieldsGrouping("partitioner", new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt())
                .localOrShuffleGrouping("fetch");

        builder.setBolt("parse", new JSoupParserBolt())
                .localOrShuffleGrouping("sitemap");

        String warcFilePath = "/tmp/foo/";

        FileNameFormat fileNameFormat = new WARCFileNameFormat()
                .withPath(warcFilePath);

        byte[] warcinfo = WARCRecordFormat.generateWARCInfo();

        WARCHdfsBolt warcbolt = (WARCHdfsBolt) new WARCHdfsBolt()
                .withFsUrl("file:///").withFileNameFormat(fileNameFormat);

        warcbolt.withHeader(warcinfo);

        builder.setBolt("warc", warcbolt).localOrShuffleGrouping("parse");

        builder.setBolt("status", new MemoryStatusUpdater())
                .localOrShuffleGrouping("fetch", Constants.StatusStreamName)
                .localOrShuffleGrouping("sitemap", Constants.StatusStreamName)
                .localOrShuffleGrouping("parse", Constants.StatusStreamName);

        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        return submit("crawl", conf, builder);
    }
}
