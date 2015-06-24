package edu.isi.dig.elasticsearch.mapreduce.driver;

import java.io.IOException;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplifyFeatures extends Reducer<Text,Text,Text,Text> {
	
	Text reusableKey=new Text();
	private static Logger LOG = LoggerFactory.getLogger(SimplifyFeatures.class);
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		for(Text value : values)
		{
			JSONObject jObjSource = (JSONObject) JSONSerializer.toJSON(value.toString());
			JSONObject jObjOutput = new JSONObject();
			
			if(jObjSource.containsKey("hasBodyPart"))
			{
				Object jBody = jObjSource.get("hasBodyPart");
				JSONArray jBodyArray = new JSONArray();
				
				if(jBody instanceof JSONObject)
				{
					jBodyArray.add(jBody);
				}
				else
				{
					jBodyArray = (JSONArray) jBody;
				}
				
				for(int i=0;i<jBodyArray.size();i++)
				{
					JSONObject jBP = jBodyArray.getJSONObject(i);
					if(jBP.containsKey("text"))
					{
						jObjOutput.accumulate("bodyText", jBP.getString("text"));
					}
					
				}
			}
			
			if(jObjSource.containsKey("hasTitlePart"))
			{
				Object jBody = jObjSource.get("hasTitlePart");
				JSONArray jTitleArray = new JSONArray();
				
				if(jBody instanceof JSONObject)
				{
					jTitleArray.add(jBody);
				}
				else
				{
					jTitleArray = (JSONArray) jBody;
				}
				
				for(int i=0;i<jTitleArray.size();i++)
				{
					JSONObject jTP = jTitleArray.getJSONObject(i);
					if(jTP.containsKey("text"))
					{
						jObjOutput.accumulate("titleText", jTP.getString("text"));
					}
					
				}
			}
			
			if(jObjSource.containsKey("hasFeatureCollection"))
			{
				JSONObject jObjFC = jObjSource.getJSONObject("hasFeatureCollection");

				@SuppressWarnings("unchecked")
				Set<String> keysFC = jObjFC.keySet();
				
				/*if(keysFC.contains("a"))
					keysFC.remove("a");
				
				if(keysFC.contains("uri"))
					keysFC.remove("uri");*/
				
				for(String keyFC : keysFC)
				{
					LOG.info("KEY:" + keyFC);
					Object jObjFeature = jObjFC.get(keyFC);
					JSONArray jArrayFC = new JSONArray();
					
					if(jObjFeature instanceof JSONObject)
					{
						jArrayFC.add(jObjFeature);
					}
					else if(jObjFeature instanceof JSONArray)
					{
						jArrayFC = (JSONArray) jObjFeature;
					}
					
					for(int i=0;i<jArrayFC.size();i++)
					{
						JSONObject jTemp = jArrayFC.getJSONObject(i);
						
						if(jTemp.containsKey("featureName") && jTemp.containsKey("featureValue"))
						{
							jObjOutput.accumulate(jTemp.getString("featureName"), jTemp.getString("featureValue"));
						}
						
					}
					
				}
				
			}
			
			if(jObjSource.containsKey("url"))
			{
				jObjOutput.accumulate("url",jObjSource.getString("url"));
			}
			
			if(jObjSource.containsKey("uri"))
			{
				LOG.info("URI:" + jObjSource.getString("uri"));
				reusableKey.set(jObjSource.getString("uri"));
				jObjOutput.accumulate("uri", jObjSource.getString("uri"));
				context.write(reusableKey, new Text(jObjOutput.toString()));
				
			}
			else
			{
				context.write(null, new Text(jObjOutput.toString()));
			}
		}
	}
}
