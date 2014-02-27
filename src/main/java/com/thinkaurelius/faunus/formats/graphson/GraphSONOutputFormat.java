package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.FaunusFileOutputFormat;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONOutputFormat extends FaunusFileOutputFormat {

    @Override
    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        DataOutputStream os = super.getDataOuputStream(job);
        final GraphSONMode mode;

        String modeStr = job.getConfiguration().getRaw("faunus.graphson.mode");

        if (modeStr.equals("normal")){
            mode = GraphSONMode.NORMAL;
        }else if (modeStr.equals("extended")){
            mode = GraphSONMode.EXTENDED;
        }else{
            mode = GraphSONMode.COMPACT;
        }

        return new GraphSONRecordWriter(os) {

            @Override
            public void write(NullWritable key, FaunusVertex vertex) throws IOException {
                if (null != vertex) {
                    this.out.write(FaunusGraphSONUtility.toJSON(vertex, mode).toString().getBytes("UTF-8"));
                    this.out.write(NEWLINE);
                }
            }
        };
    }
}

