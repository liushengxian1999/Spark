package redis;

import redis.clients.jedis.Jedis;

public class Demo1 {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("192.168.73.131", 6379);
        jedis.flushAll();
    }
}
