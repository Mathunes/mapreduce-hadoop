/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.bigramcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BigramCountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        //Recuperar número mínimo de bigramas para saída
        String minBigramOccurrence = conf.get("minBigramOccurrence");

        int sum = 0;

        for (IntWritable val : values) {
            
            sum += val.get();
            
        }

        if (sum >= Integer.parseInt(minBigramOccurrence)) {

            result.set(sum);
            context.write(key, result);

        }

    }
}
