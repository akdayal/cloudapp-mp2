import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
 
        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(PopularityCal.class);
 
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // TODO
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
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
 
    public static class PopularityCal extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        HashMap<Integer, Integer> leagueMap = new HashMap<>();
        //for refernce housekeping
        ArrayList<Integer> leagueId = new ArrayList<>();

        public void fillLeagyeMap(String path, Configuration conf) throws IOException{
            Path pt=new Path(path);
            FileSystem fs = FileSystem.get(pt.toUri(), conf);
            FSDataInputStream file = fs.open(pt);
            BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));
 
            StringBuilder everything = new StringBuilder();
            String line;
            while( (line = buffIn.readLine()) != null) {
                
                // put into map and initialize value with -1
                Integer pageId = Integer.parseInt(line.trim()); 
                leagueMap.put(pageId, -1);
                leagueId.add(pageId);
            }
            return ;
       }
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePagePath = conf.get("league");
            fillLeagyeMap(leaguePagePath, conf);
         }         

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // update leagueMap with actual populartiy value
            if(leagueMap.containsKey(key.get())) {
                leagueMap.put(key.get(), sum);
                //context.write(key, new IntWritable(sum));
            }
            
            // context.write(key, new IntWritable(sum));
 
 
        }

       @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // TODO
            // we have all the popularity of league pages in leagueMap
            //we have to rank it
            // naive way : iterate over each key and count the total number of popularity that is more than itself
            for(Integer lid : leagueId){
               int rank = 0;
               int valId = leagueMap.get(lid);
               for(Integer key : leagueMap.keySet()){
                    
                    // it self
                    if(lid == key)
                        continue;
                    if(valId > leagueMap.get(key))
                        rank ++;
               }
               context.write(new IntWritable(lid), new IntWritable(rank));
            }
            //context.write(NullWritable.get(), new TextArrayWritable(strArray));
        } 
    }
}
