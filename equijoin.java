import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class equijoin {
    public static String table1 = "";
    public static String table2 = "";
    public static class Map extends MapReduceBase implements Mapper < LongWritable, Text, DoubleWritable, Text > {
        public void map(LongWritable key, Text value, OutputCollector < DoubleWritable, Text > output, Reporter reporter) throws IOException {
            String line = value.toString();
            if(line.equals(""))
            	return;
            String lines[] = line.split(",");

            if (table1.equals(""))
                table1 = lines[0];
            else if (!table1.equals(lines[0]))
                table2 = lines[0];
            String k = lines[1];
            String v = line;
            DoubleWritable ky = new DoubleWritable(Double.parseDouble(k));
            //Text ky = new Text(k);
            Text vl = new Text(v);
            output.collect(ky, vl);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer < DoubleWritable, Text, Text, Text > {
        public void reduce(DoubleWritable key, Iterator < Text > values, OutputCollector < Text, Text > output, Reporter reporter) throws IOException {
            HashSet < String > left = new HashSet < String > ();
            HashSet < String > right = new HashSet < String > ();

            while (values.hasNext()) {
                String s = values.next().toString();
                String table = s.split(",")[0];
                if (table.equals(table1))
                    left.add(s);
                else if (table.equals(table2))
                    right.add(s);
            }

            for (String i: left) {
                for (String j: right) {
                    String newstr = i + ", " + j;
                    Text res = new Text(newstr);
                    output.collect(new Text(""), res);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(equijoin.class);
        conf.setJobName("equijoin");

        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.set("mapred.textoutputformat.separator", " ");
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
