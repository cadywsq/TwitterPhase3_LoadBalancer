import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Siqi Wang siqiw1 on 3/30/16.
 */
public class LoadBalancer extends Verticle {
    private static final String dns1 = "";
    private static final String dns2 = "";
    private static final String dns3 = "";
    private static final String dns4 = "";
    private static final String dns5 = "";
    private final Map<Integer, String> dcMap = new HashMap<Integer, String>() {{
        dcMap.put(1, dns1);
        dcMap.put(2, dns2);
        dcMap.put(3, dns3);
        dcMap.put(4, dns4);
        dcMap.put(5, dns5);
    }};

    public static void main(String[] args) {

    }

    @Override
    public void start() {
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);
        server.listen(80, "0.0.0.0");

        routeMatcher.get("/q1", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                String response = getResponse(req, key);
                req.response().end(response); // Do not remove this
            }
        });

        routeMatcher.get("/q2", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String userid = map.get("userid");
                final String hashtag = map.get("hashtag");
                String response = getResponse(req, userid + hashtag);
                req.response().end(response); // Do not remove this
            }
        });

        routeMatcher.get("/q3", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String startUserid = map.get("start_userid");
                String response = getResponse(req, startUserid);
                req.response().end(response); // Do not remove this
            }
        });

        routeMatcher.get("/q4", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String tweetId = map.get("tweetid");
                String response = getResponse(req, tweetId);
                req.response().end(response);
            }
        });
    }

    private String getResponse(HttpServerRequest req, String startUserid) {
        String dcDns = dns1;
        final String path = req.path();
        final int key = hashLocation(startUserid);
        String response = null;
        dcDns = getRedirectDNS(dcDns, key);
        try {
            response = getHttpResponse(dcDns, path);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return response;
    }

    private String getRedirectDNS(String dcDns, int key) {
        switch (key) {
            case 1:
                dcDns = dns1;
                break;
            case 2:
                dcDns = dns2;
                break;
            case 3:
                dcDns = dns3;
                break;
            case 4:
                dcDns = dns4;
                break;
        }
        return dcDns;
    }

    private String getHttpResponse(String dcDns, String path) throws MalformedURLException {
        String submitString = "http://" + dcDns + path;
        URL url = new URL(submitString);
        HttpURLConnection httpConnection = null;

        int responseCode = 0;
        while (true) {
            try {
                while (responseCode != HttpURLConnection.HTTP_OK) {
                    Thread.sleep(2);
                    httpConnection = (HttpURLConnection) url.openConnection();
                    responseCode = httpConnection.getResponseCode();
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));
                String response;
                StringBuilder builder = new StringBuilder();

                while ((response = br.readLine()) != null) {
                    builder.append(response + "\n");
                }
                builder.append("\n");
                return builder.toString();

            } catch (Exception e) {
                System.out.println("Response Code:" + responseCode);
                System.out.println("Try Again");
            }
        }
    }

    int hashLocation(String key) {
        int hash = 0;
        char[] chars = key.toCharArray();
        for (char c : chars) {
            hash += ~c;
        }
        return Math.abs(hash % 5) + 1;
    }

}
