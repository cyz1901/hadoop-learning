import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClinet {
    @Test
    public void connecthdfs_one() throws IOException {
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

    @Test
    public void connecthdfs_two() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        fs.mkdirs(new Path("/0529/dashen"));

        //关闭资源
        fs.close();
        System.out.println("over");
    }

    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        fs.copyFromLocalFile(new Path("src/main/resources/i have a dream.txt"),new Path("/test.txt"));

        fs.close();
        System.out.println("over");
    }

    @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        fs.copyToLocalFile(new Path("/test.txt"), new Path("D:/happy.txt"));

        fs.close();
        System.out.println("over");
    }

    @Test
    public void testDelete() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        fs.delete(new Path("/test.txt"),true);

        fs.close();
        System.out.println("over");
    }

    @Test
    public void testRename() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        fs.rename(new Path("/test.txt"),new Path("/happy.txt"));

        fs.close();
        System.out.println("over");
    }


    @Test
    public void testListFile() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        RemoteIterator<LocatedFileStatus> filelist =  fs.listFiles(new Path("/"),true);

        while (filelist.hasNext()){
            LocatedFileStatus filestatus = filelist.next();
            System.out.println(filestatus.getPath().getName());
            System.out.println(filestatus.getPermission());
            System.out.println(filestatus.getBlockSize());
            BlockLocation[] blockLocations = filestatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts){
                    System.out.println(host);
                }
            }

        }


        fs.close();
        System.out.println("over");
    }

    @Test
    public void putFileToHDFS() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration =new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.106:9000"),configuration,"hadoop");

        FileInputStream fileInputStream= new FileInputStream(new File("D:/happy.txt"));

        FSDataOutputStream fileOutputStream = fs.create(new Path("/happy0.txt"));

        IOUtils.copyBytes(fileInputStream,fileOutputStream,configuration);
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fileInputStream);


        fs.close();
        System.out.println("over");
    }
    public static void main(String[] args) throws IOException {

    }
}
