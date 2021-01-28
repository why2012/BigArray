package org.mine.iptable.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class IpUtils {

    public static int parse(String ip) {
        if (ip == null) {
            throw new IllegalArgumentException("invalid ip: " + ip);
        }
        String[] segments = ip.split("\\.");
        int bytes = 0;
        if (segments.length == 4) {
            bytes |= Short.parseShort(segments[0]) << 24;
            bytes |= Short.parseShort(segments[1]) << 16;
            bytes |= Short.parseShort(segments[2]) << 8;
            bytes |= Short.parseShort(segments[3]);
            return bytes;
        }
        throw new IllegalArgumentException("invalid ip: " + ip);
    }

    public static String ip(int bytes) {
        short a = (short)(bytes >>> 24);
        short b = (short)((bytes & 0x00FF0000) >>> 16);
        short c = (short)((bytes & 0x0000FF00) >>> 8);
        short d = (short)(bytes & 0x000000FF);
        return "" + a + '.' + b + '.' + c + '.' + d;
    }

    public static int byteIndicator(int bytes) {
        return bytes >>> 3;
    }

    public static int byteMask(int bytes) {
        return 1 << (bytes & 7);
    }

    public static void readIpFile(String filename, IpProcessor ipProcessor) {
        File file = new File(filename);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("illegal file: " + filename);
        }
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            String line;
            while ((line = randomAccessFile.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    ipProcessor.process(line);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public interface IpProcessor {
        void process(String ip);
    }
}
