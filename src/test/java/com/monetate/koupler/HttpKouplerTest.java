package com.monetate.koupler;

import static org.junit.Assert.assertEquals;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpKouplerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKouplerTest.class);

    @Test
    public void testRest() throws Exception {
        MockKinesisEventProducer producer = new MockKinesisEventProducer();
        Thread server = new Thread(new HttpKoupler(producer, 4567, 20));
        server.start();
        Thread.sleep(1000);

        URIBuilder builder = new URIBuilder();
        CloseableHttpClient client = HttpClients.createDefault();
        builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(4567).setPath("/event");
        HttpPost post = new HttpPost(builder.toString());
        post.setEntity(new ByteArrayEntity("foo".getBytes()));
        CloseableHttpResponse response = client.execute(post);
        String responseBody = EntityUtils.toString(response.getEntity());
        LOGGER.debug("Received [{}] as response from HTTP server.", responseBody);
        assertEquals("Did not receive valid response code", 200, response.getStatusLine().getStatusCode());
        
    }
}
