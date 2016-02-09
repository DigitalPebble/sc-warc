package com.digitalpebble.stormcrawler.warc;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;

import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;

import backtype.storm.tuple.Tuple;

public class WARCSequenceFormat extends WARCRecordFormat
        implements SequenceFormat {

    @Override
    public Class keyClass() {
        return NullWritable.class;
    }

    @Override
    public Class valueClass() {
        return WARCWritable.class;
    }

    @Override
    public Object key(Tuple tuple) {
        return NullWritable.get();
    }

    @Override
    public Object value(Tuple tuple) {
        byte[] asBytes = format(tuple);

        DataInput in = new DataInputStream(new ByteArrayInputStream(asBytes));
        try {
            return new WARCWritable(new WARCRecord(in));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
