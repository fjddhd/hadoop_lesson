import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class Mapper_fujingdong extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        //拿到一行文本并转换成String 类型
        String line = value.toString();
        //用空格分割单条日志
        String[] lines=line.split(" ");
        //获取该条日志月份
        String log_mouth=lines[0];
        //获取该条日志类型
        String log_type=lines[4];
        //统计各月份日志条数
        context.write(new Text(log_mouth),new IntWritable((1)));
        //统计个月份内所需要统计的日志类型条数
        if (log_type.contains("sendmail")){
            context.write(new Text(log_mouth+"月sendmail数量"),new IntWritable((1)));
        }
        if (log_type.contains("ctl_mboxlist")){
            context.write(new Text(log_mouth+"月ctl_mboxlist数量"),new IntWritable((1)));
        }
        if (log_type.contains("sm-msp-queue")){
            context.write(new Text(log_mouth+"月sm-msp-queue数量"),new IntWritable((1)));
        }
        if (log_type.contains("spamd")){
            context.write(new Text(log_mouth+"月spamd数量"),new IntWritable((1)));
        }
    }
}
