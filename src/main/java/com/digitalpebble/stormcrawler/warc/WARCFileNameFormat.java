package com.digitalpebble.stormcrawler.warc;

import java.util.Map;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;

/**
 * From the WARC specs It is helpful to use practices within an institution that
 * make it unlikely or impossible to duplicate aggregate WARC file names. The
 * convention used inside the Internet Archive with ARC files is to name files
 * according to the following pattern: Prefix-Timestamp-Serial-Crawlhost.warc.gz
 * Prefix is an abbreviation usually reflective of the project or crawl that
 * created this file. Timestamp is a 14- digit GMT timestamp indicating the time
 * the file was initially begun. Serial is an increasing serial-number within
 * the process creating the files, often (but not necessarily) unique with
 * regard to the Prefix. Crawlhost is the domain name or IP address of the
 * machine creating the file.
 **/

@SuppressWarnings("serial")
public class WARCFileNameFormat implements FileNameFormat {

    private int taskIndex;
    private String path = "/";
    private String prefix = "crawl";

    private final String extension = ".warc.gz";

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
        this.taskIndex = topologyContext.getThisTaskIndex();
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + "-" + timeStamp + "-" + this.taskIndex + "-"
                + rotation + this.extension;
    }

    public String getPath() {
        return this.path;
    }
}
