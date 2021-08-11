package com.dtstack.flinkx.connector.ta.util;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

public class CommonUtil {

    private static Random random = new Random();

    public static int hashIndexWithMurmur3(String data, int totalNum){
        if(data == null){
            return random.nextInt(totalNum);
        }
        HashCode hashCode = Hashing.murmur3_32().hashBytes(data.getBytes());
        return toPositive(hashCode.asInt()) % totalNum;
    }
    private static int toPositive(int number) {
        return number & 2147483647;
    }

    public static String getHostName(){
        try {
            return (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException uhe) {
            String host = uhe.getMessage();
            if (host != null) {
                int colon = host.indexOf(':');
                if (colon > 0) {
                    return host.substring(0, colon);
                }
            }
            return "UnknownHost";
        }
    }

}
