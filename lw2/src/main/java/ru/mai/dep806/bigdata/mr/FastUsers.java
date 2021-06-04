package ru.mai.dep806.bigdata.mr;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class XmlUtils {
    public static Map<String, String> parseXmlRow(String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }
        return map;
    }
}

public class FastUsers extends Configured implements Tool {
    private static final char FIELD_SEPARATOR = '\0';

    static final String[] QUESTION_FIELDS = new String[]{
            "AcceptedAnswerId", "CreationDate"
    };

    static final String[] ANSWER_FIELDS = new String[]{
            "Id", "CreationDate", "OwnerUserId"
    };

    static final String[] POST_FIELDS = new String[]{
            "QuestionCreationDate", "AnswerCreationDate", "OwnerUserId"
    };

    static final String[] USER_FIELDS = new String[]{
            "Id", "DisplayName"
    };

    static final String[] POST_MAP_FIELDS = new String[]{
            "TimeDelta"
    };

    static final String[] RESULT_FIELDS = new String[]{
            "UserId", "DisplayName", "AnswersCount"
    };

    static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static String concatenateFields(StringBuilder buffer, Map<String, String> row, char type, String[] fields) {
        buffer.setLength(0);
        if (type > 0) {
            buffer.append(type).append(FIELD_SEPARATOR); // Для обозначения строки типа User или Post
        }
        for (String field : fields) {
            String fieldValue = row.get(field);
            if (fieldValue != null) {
                buffer.append(fieldValue);
            }
            buffer.append(FIELD_SEPARATOR);
        }
        return buffer.toString();
    }


    static Map<String, String> parseFields(String buffer, String[] fields) {
        String[] values = StringUtils.split(buffer.substring(2, buffer.length() - 1), FIELD_SEPARATOR);
        return IntStream.range(0, values.length).boxed().collect(Collectors.toMap(i -> fields[i], i -> values[i]));
    }


    private static class QAMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            if (row.get("PostTypeId") != null) {

                if (row.get("PostTypeId").equals("1")) {
                    String q_id = row.get("AcceptedAnswerId");
                    if (StringUtils.isNotBlank(q_id)) {
                        outKey.set(Long.parseLong(q_id));
                        outValue.set(concatenateFields(buffer, row, 'Q', QUESTION_FIELDS));
                        context.write(outKey, outValue);
                    }
                }

                else if (row.get("PostTypeId").equals("2")) {
                    String a_id = row.get("Id");
                    if (StringUtils.isNotBlank(a_id)) {
                        outKey.set(Long.parseLong(a_id));
                        outValue.set(concatenateFields(buffer, row, 'A', ANSWER_FIELDS));
                        context.write(outKey, outValue);
                    }
                }
            }
        }

    }


    static class JoinQAReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> questions = new ArrayList<>();
            List<String> answers = new ArrayList<>();

            for (Text value : values) {
                String strValue = value.toString();
                switch (strValue.charAt(0)) {
                    case 'Q':
                        questions.add(strValue);
                        break;
                    case 'A':
                        answers.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("JoinQAReducer exception");
                }
            }

            if (questions.size() > 0 && answers.size() > 0) {
                for (String question : questions) {
                    for (String answer : answers) {
                        Map<String, String> q_row = parseFields(question, QUESTION_FIELDS);
                        Map<String, String> a_row = parseFields(answer, ANSWER_FIELDS);
                        String q_time = q_row.get("CreationDate");
                        String a_time = a_row.get("CreationDate");

                        try {
                            long diff = (DATE_FORMAT.parse(a_time).getTime() - DATE_FORMAT.parse(q_time).getTime());
                            if (diff > 1 * 1000 * 60) {
                                String userId = a_row.get("OwnerUserId");
                                if (userId != null) {
                                    outKey.set(Long.parseLong(userId));
                                    outValue.set("P" + FIELD_SEPARATOR + diff + FIELD_SEPARATOR);
                                    context.write(outKey, outValue);
                                }
                            }

                        }
                        catch (ParseException e) {
                            System.out.println("JoinQAReducer err");
                        }
                    }
                }
            }
        }
    }


    private static class UMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String keyString = row.get("Id");

            if (StringUtils.isNotBlank(keyString)) {
                outKey.set(Long.parseLong(keyString));
                outValue.set(concatenateFields(buffer, row, 'U', USER_FIELDS));
                context.write(outKey, outValue);
            }
        }
    }


    static class JoinPUReducer extends Reducer<LongWritable, Text, DoubleWritable, Text> {
        private final DoubleWritable outKey = new DoubleWritable();
        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> posts = new ArrayList<>();
            List<String> users = new ArrayList<>();

            for (Text value : values) {
                String strValue = value.toString();
                switch (strValue.charAt(0)) {
                    case 'P':
                        posts.add(strValue);
                        break;
                    case 'U':
                        users.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("JoinPUReducer err");
                }
            }

            if (posts.size() * users.size() > 1) {
                double answerTime = 0;
                String displayName = "";
                int rowCount = posts.size() * users.size();
                // Join
                for (String post : posts) {
                    for (String user : users) {
                        Map<String, String> postRow = parseFields(post, POST_MAP_FIELDS);
                        Map<String, String> userRow = parseFields(user, USER_FIELDS);
                        displayName = userRow.get("DisplayName");
                        answerTime += Long.parseLong(postRow.get("TimeDelta"));
                    }
                }

                Map<String, String> row = new HashMap<>();
                row.put("UserId", key.toString());
                row.put("DisplayName", displayName);
                row.put("AnswersCount", String.valueOf(rowCount));
                outKey.set(answerTime / (1000 * 60 * rowCount));
                outValue.set(concatenateFields(buffer, row, (char) 0, RESULT_FIELDS));
                context.write(outKey, outValue);
            }
        }
    }


    static class EMapper extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(DoubleWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }



    static class SortReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        private final Text outValue = new Text();
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> postRow = parseFields(value.toString(), RESULT_FIELDS);
                outValue.set(String.join("\t", new String[]{postRow.get("UserId"), postRow.get("DisplayName"), postRow.get("AnswersCount")}));
                context.write(key, outValue);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Path postsPath = new Path(args[0]);
        Path usersPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Path stagingPath = new Path(outputPath + "-stage-1");
        Path stagingPath2 = new Path(outputPath + "-stage-2");
        FileSystem.get(new Configuration()).delete(outputPath, true);
        Job QAJoinJob = Job.getInstance(getConf(), "Test_job");
        QAJoinJob.setJarByClass(FastUsers.class);
        QAJoinJob.setMapperClass(QAMapper.class);
        QAJoinJob.setReducerClass(JoinQAReducer.class);
        QAJoinJob.setNumReduceTasks(10);
        QAJoinJob.setOutputKeyClass(LongWritable.class);
        QAJoinJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(QAJoinJob, postsPath);
        FileOutputFormat.setOutputPath(QAJoinJob, stagingPath);
        QAJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(QAJoinJob, true);
        FileOutputFormat.setOutputCompressorClass(QAJoinJob, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(QAJoinJob, SequenceFile.CompressionType.BLOCK);


        boolean success = QAJoinJob.waitForCompletion(true);
        if (success) {
            Job UserJoinJob = Job.getInstance(getConf(), "Test_job2");
            UserJoinJob.setJarByClass(FastUsers.class);
            UserJoinJob.setReducerClass(JoinPUReducer.class);
            UserJoinJob.setNumReduceTasks(10);
            UserJoinJob.setMapOutputKeyClass(LongWritable.class);
            UserJoinJob.setMapOutputValueClass(Text.class);
            UserJoinJob.setOutputKeyClass(DoubleWritable.class);
            UserJoinJob.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(UserJoinJob, stagingPath, SequenceFileInputFormat.class, Mapper.class);
            MultipleInputs.addInputPath(UserJoinJob, usersPath, TextInputFormat.class, UMapper.class);
            FileOutputFormat.setOutputPath(UserJoinJob, stagingPath2);
            UserJoinJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileOutputFormat.setCompressOutput(UserJoinJob, true);
            FileOutputFormat.setOutputCompressorClass(UserJoinJob, SnappyCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(UserJoinJob, SequenceFile.CompressionType.BLOCK);
            success = UserJoinJob.waitForCompletion(true);

            if (success) {
                Job sortJob = Job.getInstance(getConf(), "Test_job3");
                sortJob.setJarByClass(FastUsers.class);
                sortJob.setMapperClass(EMapper.class);
                sortJob.setReducerClass(SortReducer.class);
                sortJob.setNumReduceTasks(1);
                sortJob.setOutputKeyClass(DoubleWritable.class);
                sortJob.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(sortJob, stagingPath2);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                FileOutputFormat.setOutputPath(sortJob, outputPath);
                sortJob.setOutputFormatClass(TextOutputFormat.class);
                success = sortJob.waitForCompletion(true);
            }
        }

        return success ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new FastUsers(), args);
        System.exit(result);
    }
}
