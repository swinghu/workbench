package testdrive.hadoop.joins.reduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Bill Bejeck
 * Date: 6/8/13
 * Time: 9:26 PM
 */
public class JoiningReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {

    private Text joinedText = new Text();
    private StringBuilder builder = new StringBuilder();
    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        builder.append(key.getJoinKey()).append(",");
        for (Text value : values) {
            builder.append(value.toString()).append(",");
        }
        builder.setLength(builder.length()-1);
        joinedText.set(builder.toString());
        context.write(nullKey, joinedText);
        builder.setLength(0);
    }
}
