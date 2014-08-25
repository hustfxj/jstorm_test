import java.io.File;
import java.nio.charset.Charset;
import java.util.*;

import javax.print.DocFlavor.URL;

import org.codehaus.jackson.JsonGenerator.Feature;
import org.eclipse.emf.ecore.util.EContentsEList.FeatureIterator;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;


public class TestshpFile {
	public static void main(String[] args) {  
	    try {  
	        // 第一步:我们需要给出连接Shapefile文件的参数  
	        // 并且把这些参数信息组织到一个Map实例中  
/*	        java.net.URL url = new File("conf/beijing/CHN_adm/CHN_adm/CHN_adm0.shp").toURI().toURL();  
	        Map params = new HashMap();  
	        params.put("url", url);  */
	        
	        File file = new File("conf/beijing/CHN_adm/CHN_adm/CHN_adm0.dbf");
	        ShapefileDataStore shpDataStore = new ShapefileDataStore(file.toURI().toURL());
	        shpDataStore.setCharset(Charset.forName("GBK"));
	        
	        // 还可以加入其他参数，这里以最简单的形式给出示例。  
	        // 第二步:根据刚才参数的信息，打开一个连接到Shapefile文件的数据源  
	       /* DataStore dataStore = DataStoreFinder.getDataStore(params);  */
	        // 从dataStore中获取Shapefile类型名称。  
	        // Shapefile文件名称和Shapefile类型名称通常是一样的。  
	        // 此处dataStore现在是基于Shapefile创建的, 所以TypeName就是Shapefile文件名称。  
	        String typeName = shpDataStore.getTypeNames()[0];  
	        System.out.println("::::typeName is " + typeName);  
	        // 第三步:根据Shapefile类型名称，从dataStore中获取<featurecollection 要素集合類="">的一个对象  
	        FeatureSource featureSource = shpDataStore.getFeatureSource(typeName);  
	        FeatureCollection featureCollection = featureSource.getFeatures();  
	    } catch (Exception e) {  
	        System.out.println("Ops! Something went wrong :-(");  
	        e.printStackTrace();  
	    }  
	    System.exit(0);  
	}
}
