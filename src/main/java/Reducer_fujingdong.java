import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer_fujingdong extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        //通过value这个迭代器，遍历这一组kv中所有的value，进行累加
        for(IntWritable value:values){
            count+=value.get();
        }
        //输出这个单词的统计结果
        context.write(key, new IntWritable(count));
    }
}
