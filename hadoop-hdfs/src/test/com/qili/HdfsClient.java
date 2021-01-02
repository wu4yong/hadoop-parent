package com.qili;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;


/**
 * @Date: 2021/1/2
 * @Author: wuyong 
 * @Description: hdfs客户端Api操作
 */


public class HdfsClient {
    Configuration conf = null;
    FileSystem fs = null;

    @Before
    public void beforeMethod() throws IOException {
        // 系统配置资源对象
        conf = new Configuration(true);
        conf.set("fs.defaultFS", "hdfs://node02:8020");
        // 文件系统对象
        fs = FileSystem.get(conf);
    }

    @After
    public void afterMethod() throws IOException {
        // 关闭文件系统资源
        fs.close();
    }

    @Test
    public void testupload() throws IllegalArgumentException, IOException {
        //输入流-本地文件
        FileInputStream fis = new FileInputStream("C:/Users/wu4yo/Desktop/test.txt");
        //输出流-目标hdfs上的位置
        FSDataOutputStream fsos = fs.create(new Path("/test/test.txt"));
        //工具类操作
        IOUtils.copyBytes(fis, fsos, conf);

//        //本地拷贝的方式
//        Path srcFile = new Path("C:\\Users\\wu4yo\\Desktop\\test.txt");
//        Path dst = new Path("/test/test.txt");
//        fs.copyFromLocalFile(srcFile, dst);

//     原始IO流操作方式：
//		byte[] buf = new byte[1024];
//		int len = -1;
//		while ((len = fis.read(buf)) != -1) {
//			fsos.write(buf, 0, len);
//		}
//		fis.close();
//		fsos.flush();
//		fsos.close();
    }

    @Test
    public void testdelete() throws IllegalArgumentException, IOException {
        fs.delete(new Path("/test/test.txt"));
    }

    @Test
    public void testLs() throws IllegalArgumentException, IOException {
        FileStatus fstatus = fs.getFileStatus(new Path("/mr/data/input/wc.txt"));
        System.out.println(fstatus.getBlockSize());
        System.out.println(fstatus.getOwner());
        System.out.println(fstatus.getReplication());

        BlockLocation[] bls = fs.getFileBlockLocations(fstatus, 0, fstatus.getLen());

        for (BlockLocation bl : bls) {
            System.out.println(bl);
        }
    }
}
