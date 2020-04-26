import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;


public class main {
    public static void main(String[] args) {
        //得到集群配置参数
        Configuration conf = new Configuration();
        //设置到本次的job实例中
        Job job = null;
        try {
            job = Job.getInstance(conf, "付京东-LogCount");
        } catch (IOException e) {
            System.err.println("获取job对象失败");
            e.printStackTrace();
        }
        //指定本次执行的主类是WordCount
        job.setJarByClass(main.class);
        //指定map类
        job.setMapperClass(Mapper_fujingdong.class);
        //指定reducer类
        job.setReducerClass(Reducer_fujingdong.class);
        //指定job输出的key和value的类型,如果map和reduce输出类型不完全相同，需要重新设置map的output的key和value的class类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定输入数据的路径
        try {
            FileInputFormat.addInputPath(job, new Path("/input"));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("数据输入失败");
        }
        //指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        //提交给hadoop
        try {
            job.waitForCompletion(true);
        } catch (IOException e) {
            System.err.println("IOException");
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.err.println("InterruptedException");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("ClassNotFoundException");
            e.printStackTrace();
        }
    }
}
