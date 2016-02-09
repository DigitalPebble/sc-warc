package com.digitalpebble.stormcrawler.warc;

import org.apache.hadoop.io.SequenceFile;
import org.apache.storm.hdfs.bolt.SequenceFileBolt;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;

/** SequenceFileBolt which uses GZip and a size of up to 1 GB **/
public class WARCSequenceFileBolt extends SequenceFileBolt {
    public WARCSequenceFileBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f,
                Units.GB);
        withSequenceFormat(new WARCSequenceFormat()).withRotationPolicy(rotpol);

        withCompressionType(SequenceFile.CompressionType.RECORD);
        withCompressionCodec("deflate");
    }

}
