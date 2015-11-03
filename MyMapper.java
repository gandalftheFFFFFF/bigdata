//package mrd.training.sample;
 
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
 
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
 
//import mrdp.logging.LogWriter;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
 
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
 
    private static final Log LOG = LogFactory.getLog(MyMapper.class);
 
    // Fprivate Text videoName = new Text();
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        try {
 
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is);
 
            doc.getDocumentElement().normalize();
 
            NodeList nList = doc.getElementsByTagName("timestep");
 
            for (int temp = 0; temp < nList.getLength(); temp++) {
 
                Node nNode = nList.item(temp);
 
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 
                    Element eElement = (Element) nNode;
                    
                   String time = eElement.getAttribute("time"); // Got time
                    
                    NodeList nList2 = eElement.getElementsByTagName("*"); // get nodelist of each vehicle tag in current timestep tag
                    
                    for (int t = 0; t < nList2.getLength(); t++) { // loop through
                    	Node n = nList2.item(t);
                    	if (n.getNodeType() == Node.ELEMENT_NODE) {
                    		Element e  = (Element)n;
                    		String type = e.getTagName().substring(0,1);
                    		String id = e.getAttribute("id");
                    		String x = e.getAttribute("x");
                    		String y = e.getAttribute("y");
                    		String angle = e.getAttribute("angle");
                    		String speed = e.getAttribute("speed");
                    		String pos = e.getAttribute("pos");
                    		String lane = e.getAttribute("lane");
                    		String edge = e.getAttribute("edge");
                    		
                    		// Produces CSV file :
                    		// time,type,id,x,y,angle,speed,pos,edge
                    		
                    		context.write(new Text(time + "," + type + "," + id + "," + x + "," + y + "," + angle + "," + speed + "," + pos + "," + lane + "," + edge), NullWritable.get());
                    	}
                    }
 
                }
            }
        } catch (Exception e) {
//            LogWriter.getInstance().WriteLog(e.getMessage());
        }
 
    }
 
}
