import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();  // based on kpshadoop.blogspot.com/2014/06/word-co-occurrence-problem.html
      String[] tokens = line.split("\\W+");
      String[] stopwords = new String[]{"would","said","amp","please","go","must","mr","i","me","my","myself",
      "we","our","ours","ourselves","you","new","get","tell","put","try","also","going","say","could","many","de",
      "la","et","en",
      "your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its",
      "itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those",
      "am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a",
      "an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against",
      "between","into","through","during","before","after","above","below","to","from","up","down","in","out","on",
      "off","over","under","again","further","then","once","here","there","when","where","why","how","all","any",
      "both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than",
      "too","very","s","t","can","will","just","don","should","now","https"};
      ArrayList<String>  stopList = new ArrayList<String> (Arrays.asList(stopwords));
      for(int i=0; i < tokens.length;i++){
          tokens[i] = tokens[i].trim().toLowerCase();
          if(tokens[i].contains("\\>")  || tokens[i].contains("\\<") || tokens[i].matches(".*\\d+.*") ||
          tokens[i].equalsIgnoreCase("co") || tokens[i].equalsIgnoreCase("u") || tokens[i].equalsIgnoreCase("RT")
          || tokens[i].equalsIgnoreCase("") || tokens[i].length() == 1 || tokens[i].contains("\\_") ||
          tokens[i].contains("\\t+") || stopList.contains(tokens[i])){

            continue;    
          }
          // if(stopList.contains(tokens[i]) || tokens[i].equalsIgnoreCase("the")){
          //     System.out.println("out: "+tokens[i]);
          // }
          word.set(tokens[i]);
          context.write(word,one);
      }
      // StringTokenizer itr = new StringTokenizer(value.toString());
      // while (itr.hasMoreTokens()) {
      //   String next = itr.nextToken().toLowerCase();
      //   if(next.contains("\\>") || next.contains("\\<") || next.matches(".*\\d+.*") ||
      //       next.equalsIgnoreCase("co") || next.equalsIgnoreCase("u") || next.equalsIgnoreCase("RT")
      //       || next.equalsIgnoreCase("") || next.length() == 1 || next.contains("\\_")
      //       || next.contains("\\t+")){
      //             continue;    // remove the emojis , numbers , co , u, RT , blank , single character as the first token
      //   }
      //   word.set(next);
      //   context.write(word, one);
      // }
    }
  }

  public static class IntSumReducer
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
    Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
