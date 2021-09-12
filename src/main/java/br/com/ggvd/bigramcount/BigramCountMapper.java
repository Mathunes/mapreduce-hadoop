package br.com.ggvd.bigramcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BigramCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        
        //Primeiro elemento do bigrama
        String element1 = "";
        //Segundo elemento do bigrama
        String element2 = "";

        while (itr.hasMoreTokens()) {

            element2 = itr.nextToken();
            
            word.set(element1 + ' ' + element2);
            context.write(word, one);
            
            element1 = element2;

        }

        word.set(element1);
        context.write(word, one);

    }
}
