package com.dtstack.flinkx.connector.ta.util;

import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Arrays;


/**
 * @author Felix.Wang
 * @class_name: HttpRequestUtil
 * @package: cn.thinkingdata.ta.logbus.utils
 * @describe: HTTPClient工具类
 * @creat_date: 2019/4/19
 * @creat_time: 16:44
 **/
public class HttpRequestUtil {

    private static final Logger logger = LoggerFactory.getLogger(HttpRequestUtil.class);
    private static PoolingHttpClientConnectionManager cm;
    private static SSLContext ctx = null;
    private static SSLConnectionSocketFactory socketFactory;
    private static RequestConfig requestConfig;

    private static Registry<ConnectionSocketFactory> socketFactoryRegistry;
    private static X509TrustManager xtm = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    };

    static {
        try {
            ctx = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            ctx.init(null, new TrustManager[]{xtm}, null);
        } catch (NoSuchAlgorithmException e) {
            logger.error("SSLContext init with NoSuchAlgorithmException",e);
        } catch (KeyManagementException e) {
            logger.error("SSLContext init with KeyManagementException",e);
        }
        socketFactory = new SSLConnectionSocketFactory(ctx,
                NoopHostnameVerifier.INSTANCE);
        // 创建Registry
        requestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                .setExpectContinueEnabled(Boolean.TRUE)
                .setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
                .setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC))
                .setConnectTimeout(600000)
                .setSocketTimeout(300000).build();
        socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.INSTANCE)
                .register("https", socketFactory)
                .build();
        // 创建ConnectionManager，添加Connection配置信息
        cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        cm.setDefaultMaxPerRoute(60);
        cm.setMaxTotal(100);

    }

    public static CloseableHttpClient getConnection() {

        return HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(requestConfig).build();


    }

    public static void closeHttpClient() throws IOException {
        getConnection().close();
        cm.close();
    }




}
