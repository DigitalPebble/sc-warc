package com.digitalpebble.stormcrawler.warc;

import java.util.Map;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;

import backtype.storm.task.TopologyContext;

public class WARCFileNameFormat implements FileNameFormat {

    private String componentId;
    private int taskId;
    private String path = "/";
    private String prefix = "";
    private String extension = ".warc.gz";

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public FileNameFormat withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public FileNameFormat withPath(String path) {
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + this.componentId + "-" + this.taskId + "-"
                + rotation + "-" + timeStamp + this.extension;
    }

    public String getPath() {
        return this.path;
    }
}
