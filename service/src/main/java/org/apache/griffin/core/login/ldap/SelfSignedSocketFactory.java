package org.apache.griffin.core.login.ldap;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SelfSignedSocketFactory extends SocketFactory {
    private SSLSocketFactory sf;

    public SelfSignedSocketFactory() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{new NoopTrustManager()}, null);
        sf = ctx.getSocketFactory();
    }

    public static SocketFactory getDefault() {
        try {
            return new SelfSignedSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class NoopTrustManager implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[0];
        }
    }

    @Override
    public Socket createSocket() throws IOException {
        return sf.createSocket();
    }

    @Override
    public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
        return sf.createSocket(s, i);
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws IOException, UnknownHostException {
        return sf.createSocket(s, i, inetAddress, i1);
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
        return sf.createSocket(inetAddress, i);
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) throws IOException {
        return sf.createSocket(inetAddress, i, inetAddress1, i1);
    }
}
