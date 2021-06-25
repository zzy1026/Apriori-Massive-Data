package org.apache.hadoop.examples;

import java.util.*;
import java.io.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Apriori {
    

    public static class AprioriMapper extends Mapper<Object, Text, Text, IntWritable> {  //first iteration, read C1 and generate L1
        private final static IntWritable one = new IntWritable(1);
        private static ArrayList<ArrayList<String>> frequentItem = new ArrayList<ArrayList<String>>();

        protected void setup(Context context) throws IOException, InterruptedException{

            Configuration conf = context.getConfiguration();
            String pathIn = conf.get("itemsetPath");
            Path itemPath =new Path(pathIn);
  
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(itemPath)));
            frequentItem.clear();

            String line;                    
            while ((line=br.readLine())!= null){
                String[] val = line.split("\\s+");
                ArrayList<String> items = new ArrayList<>();
                for (String tmps : val) {
                    items.add(tmps);
                }
                Collections.sort(items);
                frequentItem.add(items); 
                                    
            }
          
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\n");

            for(int j=0; j<token.length; ++j){
                String[] val = token[j].split("\\s+");
                ArrayList<String> newItemset = new ArrayList<>();
                for (String tmps : val) {
                    newItemset.add(tmps);   
                }

                for (int i = 0; i < frequentItem.size(); i++){  
                    ArrayList<String> tmpL = frequentItem.get(i);
                    if(isFrequentItemset(tmpL, newItemset)){
                        String keyItemset = "";
                        for (String str : tmpL)    keyItemset += str + " ";                       
                        context.write(new Text(keyItemset), one);
                    }
                }

            }
            
        }
    }


    public static class AprioriReducer extends Reducer<Text,IntWritable,NullWritable,Text> {
        
        private static int minSupport ;
        protected void setup(Context context) throws IOException, InterruptedException{
                   
            Configuration conf = context.getConfiguration();
            String str= conf.get("minSupport");
            minSupport = Integer.valueOf(str);
                   
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)  sum += val.get();
            if(sum >= minSupport)  context.write(NullWritable.get(), key);
            
        }
    }
    
    public static class AprioriIterMapper extends Mapper<Object, Text, Text, IntWritable> { //other iteration,  generate Ck and Lk

        private final static IntWritable one = new IntWritable(1);
        private static ArrayList<ArrayList<String>> frequentItem = new ArrayList<ArrayList<String>>();

        public static <T> ArrayList<String> union(ArrayList<String> list1,ArrayList<String> list2) {
            Set<T> set = new HashSet<T>();
            
            set.addAll((Collection<? extends T>) list1);
            set.addAll((Collection<? extends T>) list2);

            return (ArrayList<String>) new ArrayList<T>(set);
        }
        public static ArrayList<ArrayList<String>> prune(){
            ArrayList<ArrayList<String>> newFrequentItem = new ArrayList<ArrayList<String>>();
            
            
            for(int idx = 0;idx < frequentItem.size();idx++){  //cartesian product
                for(int jdx = idx + 1; jdx < frequentItem.size(); jdx++){
                    
                    ArrayList<String> jlist = (ArrayList<String>) union(frequentItem.get(idx), frequentItem.get(jdx));
                    Collections.sort(jlist);
                    if(jlist.size() == frequentItem.get(0).size() + 1){
                        newFrequentItem.add(jlist);
                    }
                }
            }
            
            Set<ArrayList<String>> uniqueLists = new HashSet<>();
            uniqueLists.addAll(newFrequentItem);
            newFrequentItem.clear();
            newFrequentItem.addAll(uniqueLists);
            ArrayList<ArrayList<String>> output = new ArrayList<ArrayList<String>>();

            for(int itemi = 0;itemi  < newFrequentItem.size(); itemi++){    //prune
                int setSum = 0;
                ArrayList<String> tmpL = newFrequentItem.get(itemi);
                
                for(int ridx = 0; ridx <tmpL.size(); ridx++){
                    ArrayList<String> tmpSet = new ArrayList(tmpL);          
                    tmpSet.remove(ridx);
                    if(frequentItem.contains(tmpSet)){
                        setSum += 1;
                    }
                }
                if(setSum == tmpL.size()){
                    output.add(tmpL);
                } 
                
            }

            return output;         
        }
        protected void setup(Context context) throws IOException, InterruptedException{
       
            Configuration conf = context.getConfiguration();
            String pathIn = conf.get("itemsetPath");
            Path itemPath =new Path(pathIn);
  
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(itemPath)));
            frequentItem.clear();
            
            String line;
            while ((line=br.readLine())!= null){
                String[] val = line.split("\\s+");
                ArrayList<String> items = new ArrayList<>();
                for (String tmps : val) items.add(tmps);               
                Collections.sort(items);
                frequentItem.add(items); 
         
            }
                    
            ArrayList<ArrayList<String>> newFrequentItem =  prune();
            frequentItem = newFrequentItem;
            
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\n");

            for(int j=0; j<token.length; ++j){
                String[] val = token[j].split("\\s+");
                ArrayList<String> new_item = new ArrayList<>();
                for (String tmps : val)  new_item.add(tmps);                 

                for (int i = 0; i < frequentItem.size(); i++){
                    ArrayList<String> tmpL = frequentItem.get(i);

                    if(isFrequentItemset(tmpL, new_item)){
                        String key_itemset = "";
                        for (String str : tmpL)  key_itemset += str + " ";
                        context.write(new Text(key_itemset), one);
                    }
                }

            }
            
        }
    }

    
    private static  boolean isFrequentItemset(ArrayList<String> tmpL,ArrayList<String> newItemset){
        boolean ishaveset = false;
        int all_same = 0;

        System.out.println(tmpL);
        for(String timpi :newItemset){
            if(tmpL.contains(timpi)){
                all_same += 1;
            }
            
        }
        if(all_same == tmpL.size()){
            ishaveset = true;
        }
        
        return ishaveset;
    }
     

    public static void main(String[] args) throws Exception {
        // set job configuration
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
    	  System.err.println("Usage: Apriori <in> <out> <itemset> <minSupport> <iterationNum>");
    	  System.exit(5);
        }

        conf.set("itemsetPath", otherArgs[2]);
        conf.set("minSupport", otherArgs[3]);

        Job job = new Job(conf, "Apriori");
        job.setJarByClass(Apriori.class);

        // set mapper's attributes
        job.setMapperClass(AprioriMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        

        // set reducer's attributes
        job.setReducerClass(AprioriReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // add file input/output path
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_0"));
        job.waitForCompletion(true);

        //----------- START ITERATION


        for(int i=1;i< Integer.valueOf(otherArgs[4]);i++){

            String x = otherArgs[1]+"_"+(i-1)+"/part-r-00000";
            conf.set("itemsetPath", x);
            Job job2 = new Job(conf, "AprioriIter");
            job2.setJarByClass(Apriori.class);

            // set mapper's attributes
            job2.setMapperClass(AprioriIterMapper.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);
            

            // set reducer's attributes
            job2.setReducerClass(AprioriReducer.class);
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"_"+i));
            job2.waitForCompletion(true);

        }
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}