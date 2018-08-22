import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.*;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCooccurPairs {

    public static class PairsMapper
    extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text wordpair = new Text();
        private String first ;
        private String second;
        private static String[] stopwords = new String[]{"would","said","amp","please","go","must","mr","i","me","my","myself",
        "we","our","ours","ourselves","you","new","get","tell","put","try","also","going","say","could","many","de",
        "la","et","en",
        "your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its",
        "itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those",
        "am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a",
        "an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against",
        "between","into","through","during","before","after","above","below","to","from","up","down","in","out","on",
        "off","over","under","again","further","then","once","here","there","when","where","why","how","all","any",
        "both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than",
        "too","very","s","t","can","will","just","don","should","now","https","like","know","re","co","t","com",
        "nytimes","feedback","page","archives","home","archives_feedback","reports","invalid","box","select",
        "newsletter","newsletter","via","email"};
        private static ArrayList<String>  stopList = new ArrayList<String> (Arrays.asList(stopwords));


        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String line = value.toString();  // based on kpshadoop.blogspot.com/2014/06/word-co-occurrence-problem.html
            String[] tokens = line.split("\\W+");

            for(int i=0; i < tokens.length; i++){
                tokens[i] = tokens[i].trim().toLowerCase();
                if(tokens[i].contains("\\>") || tokens[i].contains("\\<") || tokens[i].matches(".*\\d+.*") ||
                 tokens[i].equalsIgnoreCase("co") || tokens[i].equalsIgnoreCase("u") ||
                 tokens[i].equalsIgnoreCase("RT") || tokens[i].equalsIgnoreCase("") || tokens[i].length() == 1 ||
                 tokens[i].contains("\\_") || tokens[i].contains("\\t+") || stopList.contains(tokens[i]) ){
                    continue;
                }
                for(int j=i+1; j < tokens.length; j++){
                    tokens[j] = tokens[j].trim().toLowerCase();
                    if(tokens[j].contains("\\>") || tokens[j].contains("\\<") || tokens[j].matches(".*\\d+.*") ||
                     tokens[j].equalsIgnoreCase("co") || tokens[j].equalsIgnoreCase("u") ||
                     tokens[j].equalsIgnoreCase("RT") || tokens[j].equalsIgnoreCase("") || tokens[j].length() == 1 ||
                     tokens[j].contains("\\_") || tokens[j].contains("\\t+") || stopList.contains(tokens[j]) ){
                        continue;
                    }

                    first = tokens[i];
                    second = tokens[j];

                    wordpair.set(first+","+second);
                    context.write(wordpair,one);
                }

            }

        }
    }

    public static class PairSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {



        private Map<Text,IntWritable> countMap = new HashMap<Text,IntWritable>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            IntWritable result = new IntWritable();
            result.set(sum);
            //System.out.println("key: "+key+" value: "+result);
            Text key2 = new Text(key);
            countMap.put(key2,result);
            // for(Text key1: countMap.keySet()){
            //   System.out.println("keyMAP: "+key1+" valueMAP: "+countMap.get(key1));
            // }

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            //Map sortedMap = sortByValues(countMap);

            //System.out.println("countmap size:  "+countMap.size());

            // for(Text key: countMap.keySet()){
            //   System.out.println("key: "+key+" value: "+countMap.get(key));
            // }

            List<Map.Entry<Text,IntWritable>> entries = new LinkedList<Map.Entry<Text,IntWritable>>(countMap.entrySet());

            // for(Map.Entry<Text,IntWritable> entry: entries){
            //   System.out.println("key: "+entry.getKey()+" value: "+entry.getValue());
            // }

            Collections.sort(entries, new Comparator<Map.Entry<Text,IntWritable>>() {

                @Override
                public int compare(Entry<Text,IntWritable> o1, Entry<Text,IntWritable> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //System.out.println("entries size:  "+entries.size());
            //LinkedHashMap will keep the keys in the order they are inserted
            //which is currently sorted on natural ordering
            Map<Text,IntWritable> sortedMap = new LinkedHashMap<Text,IntWritable>();
            int counter = 0;
            // for(Map.Entry<Text,IntWritable> entry: entries){
            //   if (counter ++ == 10) {
            //       break;
            //   }
            //   System.out.println("key: "+entry.getKey()+" value: "+entry.getValue()+" counter: "+counter);
            //   context.write(entry.getKey(), entry.getValue());
            // }
            for(Map.Entry<Text,IntWritable> entry: entries){
              sortedMap.put(entry.getKey(), entry.getValue());
            }
            //System.out.println("sortedMap size:  "+sortedMap.size());


             for (Text key: sortedMap.keySet()) {
                //System.out.println("key: "+key+" sortedMap.get(key): "+sortedMap.get(key)+" counter: "+counter);
                if (counter ++ == 15) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
          }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word co-occurrence - Pairs Approach");
        job.setJarByClass(WordCooccurPairs.class);
        job.setMapperClass(PairsMapper.class);
        job.setCombinerClass(PairSumReducer.class);
        job.setReducerClass(PairSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
