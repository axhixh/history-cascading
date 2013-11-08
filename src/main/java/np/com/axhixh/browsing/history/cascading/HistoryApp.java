package np.com.axhixh.browsing.history.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.util.Properties;

public class HistoryApp {

    public static void main(String[] args) {
        processAll();
        processFilter();
    }

    static void processAll() {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, HistoryApp.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        Tap out = new Hfs(new TextDelimited(true, "\t"), "output/history");
        Tap history = new Hfs(new TextDelimited(true, ","), "data/moz_historyvisits.csv");
        Pipe visitPipe = new Each("visit", Fields.ALL, new TimeSplit(), Fields.RESULTS);

        Pipe countPipe = new Pipe("count", visitPipe);
        countPipe = new GroupBy(countPipe, TimeSplit.OUTPUT);
        countPipe = new Every(countPipe, Fields.ALL, new Count(), Fields.ALL);

        FlowDef flowDef = FlowDef.flowDef()
                .setName("all")
                .addSource(visitPipe, history)
                .addTailSink(countPipe, out);

        Flow flow = flowConnector.connect(flowDef);
        // flow.writeDOT("output/history.dot");
        flow.complete();
    }

    static void processFilter() {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, HistoryApp.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);


        Tap placesSource = new Hfs(new TextDelimited(true, "\t"), "data/moz_places_filtered.txt");
        Pipe filteredPlaces = new Retain(new Each("places", Fields.ALL, new URLFilter()),
                new Fields("id", "url"));

        Tap history = new Hfs(new TextDelimited(true, ","), "data/moz_historyvisits.csv");
        Pipe visits = new Retain(new Pipe("visits"), new Fields("place_id", "visit_date"));

        Pipe filteredPipe = new HashJoin(filteredPlaces, new Fields("id"), visits, new Fields("place_id"));
        Pipe timePipe = new Each(filteredPipe, Fields.ALL, new TimeSplit(), Fields.RESULTS);
        Pipe countPipe = new Pipe("count", timePipe);
        countPipe = new GroupBy(countPipe, TimeSplit.OUTPUT);
        countPipe = new Every(countPipe, Fields.ALL, new Count(), Fields.ALL);

        Tap out = new Hfs(new TextDelimited(true, "\t"), "output/filtered");
        FlowDef flowDef = FlowDef.flowDef()
                .setName("filtered")
                .addSource(visits, history)
                .addSource(filteredPlaces, placesSource)
                .addTailSink(countPipe, out);

        Flow flow = flowConnector.connect(flowDef);
        // flow.writeDOT("output/filter.dot");
        flow.complete();
    }
}
