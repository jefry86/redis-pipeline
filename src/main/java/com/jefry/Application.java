package com.jefry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Application {

    public static void splitFile(String filePath, int fileCount) throws IOException {
        FileInputStream fis = new FileInputStream(filePath);
        FileChannel inputChannel = fis.getChannel();
        final long fileSize = inputChannel.size();
        long average = fileSize / fileCount;//平均值
        long bufferSize = 200; //缓存块大小，自行调整
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.valueOf(bufferSize + "")); // 申请一个缓存区
        long startPosition = 0; //子文件开始位置
        long endPosition = average < bufferSize ? 0 : average - bufferSize;//子文件结束位置
        for (int i = 0; i < fileCount; i++) {
            if (i + 1 != fileCount) {
                int read = inputChannel.read(byteBuffer, endPosition);// 读取数据
                readW:
                while (read != -1) {
                    byteBuffer.flip();//切换读模式
                    byte[] array = byteBuffer.array();
                    for (int j = 0; j < array.length; j++) {
                        byte b = array[j];
                        if (b == 10 || b == 13) { //判断\n\r
                            endPosition += j;
                            break readW;
                        }
                    }
                    endPosition += bufferSize;
                    byteBuffer.clear(); //重置缓存块指针
                    read = inputChannel.read(byteBuffer, endPosition);
                }
            } else {
                endPosition = fileSize; //最后一个文件直接指向文件末尾
            }

            FileOutputStream fos = new FileOutputStream(filePath + "_" + (i + 1));
            FileChannel outputChannel = fos.getChannel();
            inputChannel.transferTo(startPosition, endPosition - startPosition, outputChannel);//通道传输文件数据
            outputChannel.close();
            fos.close();
            startPosition = endPosition + 1;
            endPosition += average;
        }
        inputChannel.close();
        fis.close();
    }

    public static void wireFile(String outputFile) {
        FileOutputStream out;
        OutputStreamWriter writer;
        BufferedWriter bw = null;

        try {
            out = new FileOutputStream(outputFile, false);
            writer = new OutputStreamWriter(out);
            bw = new BufferedWriter(writer);
            for (int i = 1; i < 1000; i++) {
                String content = "1qda9NEwRTon0LFpXige7" + i;
                bw.write(content);
                bw.newLine();
            }
            bw.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("生成测试数据结束");
    }

    public static void main(String[] args) {
        org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml();
        ConfEntity confEntity = yaml.loadAs(Application.class.getResourceAsStream("/application.yml"), ConfEntity.class);
        try {
            String filename = confEntity.getFile().get("path");
            if ("1".equals(confEntity.getFile().get("is_tmp"))) {
                wireFile(filename);
            }
            int fileCount = Integer.parseInt(confEntity.getFile().get("count"));
            splitFile(filename, fileCount);
            for (int i = 1; i <= fileCount; i++) {
                PipelineRunnable pipelineRunnable = new PipelineRunnable();
                pipelineRunnable.setDb(Integer.parseInt(confEntity.getRedis().get("db")));
                pipelineRunnable.setHost(confEntity.getRedis().get("host"));
                pipelineRunnable.setPassword(confEntity.getRedis().get("password"));
                pipelineRunnable.setPort(Integer.parseInt(confEntity.getRedis().get("port")));
                pipelineRunnable.setFilePath(filename);
                pipelineRunnable.filePath = filename + "_" + i;


                Thread thread = new Thread(pipelineRunnable, "pipeline_runnable_" + i);
                thread.join();
                thread.start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
