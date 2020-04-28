package com.jefry;

import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.*;

@Data
public class PipelineRunnable implements Runnable {

    private String host;
    private int port;
    private String password;
    private int db;
    private String prefix;
    public String filePath;

    public void run() {
        Jedis jedis = new Jedis(host, port);
        jedis.auth(password);
        jedis.select(db);
        Pipeline pipelined = jedis.pipelined();
        long begin = System.currentTimeMillis();
        //读取文件内容
        File file = new File(filePath);
        try {
            BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "utf-8"), 5 * 1024 * 1024);// 用5M的缓冲读取文本文件
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                String key = prefix + line.trim();
                pipelined.set(key, "1");
                i++;
            }

            pipelined.sync();
            jedis.close();
            fis.close();
            reader.close();
            long end = System.currentTimeMillis();
            System.out.println(filePath + " use pipeline batch set total time：" + (end - begin) + ",count:" + i);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            file.delete();
        }


    }
}
