import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSClinet {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.1.106:9000");
        //获取hdfs客户端对象
        FileSystem fs = FileSystem.get(conf);
        //在hdfs上创建路径
        fs.mkdirs(new Path("/0529/dashen"));

        //关闭资源
        fs.close();
        System.out.println("over");
    }
}
