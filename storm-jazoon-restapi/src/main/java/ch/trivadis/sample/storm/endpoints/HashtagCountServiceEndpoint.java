package ch.trivadis.sample.storm.endpoints;


import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;

@Component
@Produces("text/plain")
public class HashtagCountServiceEndpoint extends AbstractServiceEndpoint {

	private Jedis jedis;
	
	public HashtagCountServiceEndpoint() {
		jedis = new Jedis("localhost", 6379, 1800);
		jedis.connect();
	}
	
    
    @GET
    @Path("hashtagCounts")
    public String getHashtagCounts() {
    	Map<String,String> hashtagCounts = jedis.hgetAll("jfs2013:hashtags");
    	
        StringBuffer response = new StringBuffer();
        response.append("hashtag").append("\t").append("count").append("\n");
    	for (String hashtag : hashtagCounts.keySet()) {
    		if (!hashtag.equals("jfs2013"))
    			response.append(hashtag).append("\t").append(hashtagCounts.get(hashtag)).append("\n");
    	}
    	return response.toString();
    }
}
