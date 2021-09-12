package br.com.ggvd.bigramcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramCount {

    public static void main(String[] args) throws Exception {

        //Criar uma configuração
        Configuration conf = new Configuration();

        //Obter número mínimo de ocorrências de bigramas para saída
        String minBigramOccurrence = args[0];
        Path inputPath = new Path(args[1]);
        Path outputDir = new Path(args[2]);

        conf.set("minBigramOccurrence", minBigramOccurrence);

        //Criar o job
        Job job = Job.getInstance(conf, "bigram count");
        job.setJarByClass(BigramCount.class);

        //Map e reduce
        job.setMapperClass(BigramCountMapper.class);
        job.setReducerClass(BigramCountReducer.class);
        job.setNumReduceTasks(1);

        //Definir chaves e valores
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Entradas
        FileInputFormat.addInputPath(job, inputPath);

        //Saídas
        FileOutputFormat.setOutputPath(job, outputDir);

        //Excluir saída se existir
        FileSystem hdfs = FileSystem.get(conf);
        
        if (hdfs.exists(outputDir)) {
            
            hdfs.delete(outputDir, true);
            
        }

        //Executar
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
