package com.digitalpebble.stormcrawler.warc;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.hdfs.bolt.format.RecordFormat;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;

import org.apache.storm.tuple.Tuple;

/** Generate a byte representation of a WARC entry from a tuple **/
@SuppressWarnings("serial")
public class WARCRecordFormat implements RecordFormat {

    private static final String WARC_VERSION = "WARC/1.0";
    private static final String CRLF = "\r\n";
    private static final byte[] CRLF_BYTES = { 13, 10 };

    private static final SimpleDateFormat warcdf = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

    private static final Base32 base32 = new Base32();
    private static final String digestNoContent = getDigestSha1(new byte[0]);

    public static String getDigestSha1(byte[] bytes) {
        return "sha1:" + base32.encodeAsString(DigestUtils.sha1(bytes));
    }

    public static String getDigestSha1(byte[] bytes1, byte[] bytes2) {
        MessageDigest sha1 = DigestUtils.getSha1Digest();
        sha1.update(bytes1);
        return "sha1:" + base32.encodeAsString(sha1.digest(bytes2));
    }

    /**
     * Generates a WARC info entry which can be stored at the beginning of each
     * WARC file.
     **/
    public static byte[] generateWARCInfo(Map<String, String> fields) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        buffer.append("WARC-Type: warcinfo").append(CRLF);

        Date now = new Date();
        buffer.append("WARC-Date").append(": ").append(warcdf.format(now))
                .append(CRLF);

        String mainID = UUID.randomUUID().toString();

        buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
                .append(mainID).append(">").append(CRLF);

        buffer.append("Content-Type").append(": ")
                .append("application/warc-fields").append(CRLF);

        StringBuilder fieldsBuffer = new StringBuilder();

        // add WARC fields
        // http://bibnum.bnf.fr/warc/WARC_ISO_28500_version1_latestdraft.pdf
        Iterator<Entry<String, String>> iter = fields.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            fieldsBuffer.append(entry.getKey()).append(": ")
                    .append(entry.getValue()).append(CRLF);
        }

        buffer.append("Content-Length").append(": ").append(
                fieldsBuffer.toString().getBytes(StandardCharsets.UTF_8).length)
                .append(CRLF);

        buffer.append(CRLF);

        buffer.append(fieldsBuffer.toString());

        buffer.append(CRLF);
        buffer.append(CRLF);

        return buffer.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] format(Tuple tuple) {

        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // were the headers stored as is? Can write a response element then
        String headersVerbatim = metadata.getFirstValue("_response.headers_");
        byte[] httpheaders = new byte[0];
        if (StringUtils.isNotBlank(headersVerbatim)) {
            // check that ends with an empty line
            if (!headersVerbatim.endsWith(CRLF + CRLF)) {
                headersVerbatim += CRLF + CRLF;
            }
            httpheaders = headersVerbatim.getBytes();
        }

        StringBuffer buffer = new StringBuffer();
        buffer.append(WARC_VERSION);
        buffer.append(CRLF);

        String mainID = UUID.randomUUID().toString();

        buffer.append("WARC-Record-ID").append(": ").append("<urn:uuid:")
                .append(mainID).append(">").append(CRLF);

        int contentLength = 0;
        String payloadDigest = digestNoContent;
        String blockDigest;
        if (content != null) {
            contentLength = content.length;
            payloadDigest = getDigestSha1(content);
            blockDigest = getDigestSha1(httpheaders, content);
        } else {
            blockDigest = getDigestSha1(httpheaders);
        }

        // add the length of the http header
        contentLength += httpheaders.length;

        buffer.append("Content-Length").append(": ")
                .append(Integer.toString(contentLength)).append(CRLF);

        // TODO get actual fetch time from metadata if any
        Date now = new Date();
        buffer.append("WARC-Date").append(": ").append(warcdf.format(now))
                .append(CRLF);

        // check if http headers have been stored verbatim
        // if not generate a response instead
        String WARCTypeValue = "resource";

        if (StringUtils.isNotBlank(headersVerbatim)) {
            WARCTypeValue = "response";
        }

        buffer.append("WARC-Type").append(": ").append(WARCTypeValue)
                .append(CRLF);

        // "WARC-IP-Address" if present
        String IP = metadata.getFirstValue("_ip_");
        if (StringUtils.isNotBlank(IP)) {
            buffer.append("WARC-IP-Address").append(": ").append("IP")
                    .append(CRLF);
        }

        String targetURI = null;

        // must be a valid URI
        try {
            String normalised = url.replaceAll(" ", "%20");
            URI uri = URI.create(normalised);
            targetURI = uri.toASCIIString();
            buffer.append("WARC-Target-URI").append(": ").append(targetURI)
                    .append(CRLF);
        } catch (Exception e) {
            throw new RuntimeException("Invalid URI " + url);
        }

        // provide a ContentType if type response
        if (WARCTypeValue.equals("response")) {
            buffer.append("Content-Type: application/http; msgtype=response")
                    .append(CRLF);
        }
        // for resources just use the content type provided by the server if any
        else {
            String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);
            if (StringUtils.isBlank(ct)) {
                ct = "application/octet-stream";
            }
            buffer.append("Content-Type: ").append(ct).append(CRLF);
        }

        buffer.append("WARC-Payload-Digest").append(": ").append(payloadDigest)
                .append(CRLF);
        buffer.append("WARC-Block-Digest").append(": ").append(blockDigest)
                .append(CRLF);

        // finished writing the WARC headers, now let's serialize it
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try {
            // store the headers
            bos.write(buffer.toString().getBytes(StandardCharsets.UTF_8));
            bos.write(CRLF_BYTES);
            // the http headers
            bos.write(httpheaders);

            // the binary content itself
            if (content != null) {
                bos.write(content);
            }
            bos.write(CRLF_BYTES);
            bos.write(CRLF_BYTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return bos.toByteArray();
    }

}
