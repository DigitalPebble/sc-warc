package com.digitalpebble.stormcrawler.warc;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

@SuppressWarnings("serial")
public class WARCHdfsBolt extends GzipHdfsBolt {

    private byte[] header;

    public WARCHdfsBolt() {
        super();
        FileSizeRotationPolicy rotpol = new FileSizeRotationPolicy(1.0f,
                Units.GB);
        withRecordFormat(new WARCRecordFormat()).withRotationPolicy(rotpol);
        // dummy sync policy
        withSyncPolicy(new CountSyncPolicy(1000));
        // default local filesystem
        withFsUrl("file:///");
    }

    public WARCHdfsBolt withHeader(byte[] header) {
        this.header = header;
        return this;
    }

    protected Path createOutputFile() throws IOException {
        Path path = super.createOutputFile();

        // write the header at the beginning of the file
        if (header != null && header.length > 0) {
            writeRecord(header);
        }

        return path;
    }

}
