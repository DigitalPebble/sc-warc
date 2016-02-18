package com.digitalpebble.stormcrawler.warc;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;

@SuppressWarnings("serial")
public class WARCHdfsBolt extends HdfsBolt {

    public WARCHdfsBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f,
                Units.GB);
        withRecordFormat(new WARCRecordFormat()).withRotationPolicy(rotpol);
    }

}
