package com.dtstack.flinkx.connector.ta.util;

import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;
import org.apache.commons.codec.binary.Base64;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Created by zhoujin on 2018/7/24.
 */
public class CompressUtil {
    public static byte[] lzoCompress(byte[] srcBytes) throws IOException {
        LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(
                LzoAlgorithm.LZO1X, null);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        LzoOutputStream cs = new LzoOutputStream(os, compressor);
        cs.write(srcBytes);
        cs.close();
        return os.toByteArray();
    }


    public static byte[] lz4Compress(byte[] srcBytes) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(
                byteOutput, 2048, compressor);
        compressedOutput.write(srcBytes);
        compressedOutput.close();
        return byteOutput.toByteArray();
    }

    public static byte[] snappyCompress(byte[] srcBytes) throws IOException {
        return Snappy.compress(srcBytes);
    }

    public static byte[] gzipCompress(byte[] srcBytes) throws IOException {
        GZIPOutputStream gzipOut = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            gzipOut = new GZIPOutputStream(out);
            gzipOut.write(srcBytes);
            gzipOut.close();
            return out.toByteArray();
        }finally {
            if(gzipOut != null){
                gzipOut.close();
            }
        }

    }

    /**
     * 老版本的logagent压缩
     * */
    public static byte[] gzipCompress4Logagent(byte[] srcBytes) throws IOException {
        //compress
        ByteArrayOutputStream byteArrayBuffer = new ByteArrayOutputStream();
        try {
            GZIPOutputStream var2 = new GZIPOutputStream(byteArrayBuffer);
            var2.write(srcBytes);
            var2.close();
        }catch(IOException var3) {
            throw new IOException("GZIP compress with exception "+var3.getCause().getMessage());
        }

        return Base64.encodeBase64(byteArrayBuffer.toByteArray());
    }

}
