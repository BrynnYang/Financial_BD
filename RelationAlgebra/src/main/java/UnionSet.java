import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * �󽻼�������ÿ��record����(record,1)��reduce
 * ʱֵΪ2�ŷ����record
 * @author KING
 *
 */
public class UnionSet {
    public static class UnionSetMap extends Mapper<LongWritable, Text, RelationA, IntWritable>{
        private IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable offSet, Text line, Context context)throws
                IOException, InterruptedException{
            RelationA record = new RelationA(line.toString());
            context.write(record, one);
        }
    }
    public static class UnionSetReduce extends Reducer<RelationA, IntWritable, RelationA, NullWritable>{
        @Override
        public void reduce(RelationA key, Iterable<IntWritable> value, Context context) throws
                IOException,InterruptedException{
            int sum = 0;
            for(IntWritable val : value){
                sum += val.get();
            }
            if(sum == 2 || sum == 1)
                context.write(key, NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Job unionsetJob = new Job();
        unionsetJob.setJobName("unionsetJob");
        unionsetJob.setJarByClass(UnionSet.class);
        unionsetJob.setMapperClass(UnionSetMap.class);
        unionsetJob.setMapOutputKeyClass(RelationA.class);
        unionsetJob.setMapOutputValueClass(IntWritable.class);

        unionsetJob.setReducerClass(UnionSetReduce.class);
        unionsetJob.setOutputKeyClass(RelationA.class);
        unionsetJob.setOutputValueClass(NullWritable.class);

        unionsetJob.setInputFormatClass(TextInputFormat.class);
        unionsetJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(unionsetJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(unionsetJob, new Path(args[1]));

        unionsetJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}
