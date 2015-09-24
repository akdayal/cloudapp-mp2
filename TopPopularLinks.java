import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.lang.*;
import java.util.*;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(this.getConf(), "Orphan Pages");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);
 
        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);
 
        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);
 
        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
 
        jobA.setJarByClass(TopPopularLinks.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(this.getConf(), "Orphan Pages");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);
 
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);
 
        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
 
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
 
        jobB.setJarByClass(TopPopularLinks.class);
        return jobB.waitForCompletion(true) ? 0: 1;

    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        // TODO
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value will be in this format
            // 2: 3 747213 1664968 1691047 4095634 5535664
            String line = value.toString();
            String pageIds[] = line.split(":");
            String srcPageId = pageIds[0].trim();
 
            // put srcPageID with 0 value
            context.write(new IntWritable(Integer.parseInt(srcPageId)), new IntWritable(0));
            String[] outPages = pageIds[1].trim().split(" ");
            for(String outpageId :outPages ){
                 context.write(new IntWritable(Integer.parseInt(outpageId.trim())), new IntWritable(1));
            }
 
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // TODO
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        TreeSet<Pair> tree = new TreeSet<Pair>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        // TODO
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            
            Pair <Integer, Integer> p = new Pair<>(Integer.valueOf(value.toString()), Integer.valueOf(key.toString()));
            tree.add(p);
        }
 
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            // get top 10, N, element from Treeset and send it
            TreeSet<Pair> treereverse = new TreeSet<Pair>();
            treereverse = (TreeSet)tree.descendingSet();
 
            Iterator itr = treereverse.iterator();
            int i = 0;
            int copySize = Math.min(tree.size(), N);
            //Integer[] intArray = new Integer[copySize];
            while (itr.hasNext() && i != copySize){
                 Pair p = (Pair)itr.next();
                 Integer[] ints = new Integer[2];
                 //send top N entry
                 //strArray[i] = p.first.toString()+ "|" +  p.second.toString();
                 ints[0] =Integer.parseInt(p.first.toString());
                 ints[1] = Integer.parseInt(p.second.toString());
                 context.write(NullWritable.get(), new IntArrayWritable(ints));
                 i ++;
            }
           // context.write(NullWritable.get(), new IntArrayWritable(strArray));
 
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        TreeSet<Pair> tree = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }
        // TODO
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            
            for(IntArrayWritable value : values){
                //for(Writable  w : value.get()){
                 //  String str = ((Text)w).toString();
                  // String arrStr[] = str.split("\\|");
                   Writable[] ints = value.get();
                   tree.add(new Pair((IntWritable)ints[0], (IntWritable)ints[1]));
               //}
            }
 
            // reverse tree set
            TreeSet<Pair> treereverse = new TreeSet<Pair>();
            treereverse = (TreeSet)tree.descendingSet();
            Iterator itr = treereverse.iterator();
            int count = 1;
            while (itr.hasNext() && count != N + 1){
                 Pair p = (Pair)itr.next();
                 //send top N entry
                 context.write(new IntWritable(Integer.parseInt(p.second.toString())), new IntWritable(Integer.parseInt(p.first.toString())));
                 count ++;
            }
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
