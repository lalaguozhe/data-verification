package com.dinaping.hive;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatMultiInputFormat;
import org.apache.hcatalog.mapreduce.HCatMultiSplit;
import org.apache.hcatalog.mapreduce.InputJobInfo;

import java.io.IOException;
import java.util.*;

/**
 * Created by hadoop on 14-4-15.
 */
public class DataVerification extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(DataVerification.class);

    public static final String TABLES_NAMES = "TABLES_NAMES";

    public static class DataVerificationMap extends Mapper<WritableComparable, HCatRecord, Text, MapValueData> {

        private HCatSchema schema;
        private List<HCatFieldSchema> fieldsSchema;
        private String tableName;
        private Map<Text, Integer> recordToCountMap;
        private MapValueData valueData;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            recordToCountMap = new HashMap<Text, Integer>(1000);
            valueData = new MapValueData();

            HCatMultiSplit split = (HCatMultiSplit) context.getInputSplit();
            tableName = split.getTableName();
            schema = HCatMultiInputFormat.getTableSchema(context, tableName);
            if (schema == null) {
                throw new RuntimeException("schema is empty, table name is " + tableName);
            }
            fieldsSchema = schema.getFields();

            LOG.info("input split table name :" + tableName);
            for (int i = 0; i < fieldsSchema.size(); i++) {
                HCatFieldSchema fieldSchema = fieldsSchema.get(i);
                LOG.info(String.format("field %s name:%s type:%s", i, fieldSchema.getName(), fieldSchema.getTypeString()));
            }
        }

        @Override
        protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
            Integer count = recordToCountMap.get(value);
            Text keyText = new Text(StringUtils.join(value.getAll(), '\001'));
            if (count == null) {
                recordToCountMap.put(keyText, 1);
            } else {
                recordToCountMap.put(keyText, count + 1);
            }

            if (recordToCountMap.size() > 1000) {
                flushToMapOutputBuffer(context);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!recordToCountMap.isEmpty()) {
                flushToMapOutputBuffer(context);
            }
        }

        private void flushToMapOutputBuffer(Context context) throws IOException, InterruptedException {
            LOG.info("start to flush data to map output buffer, recordToCountMap size:" + recordToCountMap.size());
            for (Map.Entry<Text, Integer> entry : recordToCountMap.entrySet()) {
                valueData.setTableName(tableName);
                valueData.setPartialCount(entry.getValue());
                context.write(entry.getKey(), valueData);
            }
            recordToCountMap.clear();
        }
    }

    public static class DataVerificationReduce extends Reducer<Text, MapValueData, Text, Text> {
        private Map<String, Integer> tableToCount;
        private Text keyText;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            tableToCount = new HashMap<String, Integer>(2);
            keyText = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<MapValueData> values, Context context) throws IOException, InterruptedException {
            for (MapValueData value : values) {
                int partialCount = value.getPartialCount();
                String tableName = value.getTableName();

                Integer currentCount = tableToCount.get(tableName);
                if (currentCount == null) {
                    tableToCount.put(tableName, partialCount);
                } else {
                    tableToCount.put(tableName, currentCount + partialCount);
                }
            }

            if (tableToCount.size() == 1) {
                String tableName = tableToCount.keySet().iterator().next();
                int totalCount = tableToCount.get(tableName);
                keyText.set(String.format("%s:%s", tableName, totalCount));
                context.write(keyText, key);
            } else if (tableToCount.size() == 2) {
                Iterator iter = tableToCount.keySet().iterator();
                String firstTableName = (String) iter.next();
                String secondTableName = (String) iter.next();
                int rowCountInFirstTbl = tableToCount.get(firstTableName);
                int rowCountInSecondTbl = tableToCount.get(secondTableName);
                if (rowCountInFirstTbl != rowCountInSecondTbl) {
                    keyText.set(String.format("%s:%s,%s:%s", firstTableName, rowCountInFirstTbl, secondTableName, rowCountInSecondTbl));
                    context.write(keyText, key);
                }
            } else {
                throw new RuntimeException("table names count should not be " + tableToCount.size());
            }
            tableToCount.clear();
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        CommandLine cmd = parseArgs(new GenericOptionsParser(conf, args)
                .getRemainingArgs());
        String[] table1 = StringUtils.split(cmd.getOptionValue("t1"), '.');
        String[] table2 = StringUtils.split(cmd.getOptionValue("t2"), '.');
        String diffPath = cmd.getOptionValue("dp");
        int reduceNumber = Integer.parseInt(cmd.getOptionValue("rn", "10"));

        Job job = new Job(conf, "Data Verification Job");
        job.getConfiguration().set(TABLES_NAMES, cmd.getOptionValue("t1") + ":" + cmd.getOptionValue("t2"));
        ArrayList<InputJobInfo> inputJobInfoList = new ArrayList<InputJobInfo>(2);
        inputJobInfoList.add(InputJobInfo.create(table1[0], table1[1], null, null));
        inputJobInfoList.add(InputJobInfo.create(table2[0], table2[1], null, null));

        HCatMultiInputFormat.setInput(job, inputJobInfoList);
        job.setInputFormatClass(HCatMultiInputFormat.class);
        job.setJarByClass(DataVerification.class);
        job.setMapperClass(DataVerificationMap.class);
        job.setReducerClass(DataVerificationReduce.class);
        job.setNumReduceTasks(reduceNumber);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapValueData.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outPath = new Path(diffPath);

        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();

        Option o = new Option("t1", "table1", true, "table one for comparison, specify it like dbname.tablename");
        o.setArgName("table1");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("t2", "table2", true, "table two for comparison, specify it like dbname.tablename");
        o.setArgName("table2");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("dp", "diffpath", true, "path which contains different data from two tables, empty content means there is no difference");
        o.setArgName("diffpath");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("rn","reducenumber", true, "reduce number");
        o.setArgName("reducenumber");
        o.setRequired(true);
        options.addOption(o);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOG.error(e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Data verification", options, true);
            System.exit(1);
        }
        return cmd;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DataVerification(), args);
        System.exit(exitCode);
    }
}