package edu.isi.dig.elasticsearch.mapreduce.driver;

import java.io.IOException;
import java.util.ArrayList;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImageFeatureReducer extends Reducer<Text,Text,Text,Text>  {
	
	Text reusableKey=new Text();
	private static Logger LOG = LoggerFactory.getLogger(ImageFeatureReducer.class);
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		for(Text value:values)
		{
			ArrayList<String> featureList = featureList();
			JSONObject jOutput = new JSONObject();
			
			JSONObject jObj =  (JSONObject)JSONSerializer.toJSON(value.toString());
			//LOG.info("JOBJ:" + jObj.toString());
			
			if(jObj.containsKey("hasImagePart"))
			{
				JSONArray jImageArray = new JSONArray();
				JSONObject jObjImage = new JSONObject();
				
				Object jImage= jObj.get("hasImagePart");
				
				if(jImage instanceof JSONObject)
				{
					jImageArray.add(jImage);
				}
				else if(jImage instanceof JSONArray)
				{
					jImageArray = (JSONArray)jImage;
				}
				
				
				for(int i=0;i<jImageArray.size();i++)
				{
					jObjImage = jImageArray.getJSONObject(i);
					
					if(jObj.containsKey("hasFeatureCollection"))
					{
						JSONObject jFeatureObject = jObj.getJSONObject("hasFeatureCollection");
						
						for(int j=0;j<featureList.size();j++)
						{
							if(jFeatureObject.containsKey(featureList.get(j)))
							{
								jObjImage.accumulate(featureList.get(j), jFeatureObject.get(featureList.get(j)));
							}
						}
						
					}
					
				}
				
				jOutput.accumulate("hasImagePart", jObjImage);
				
			}
			
			if(jObj.containsKey("uri"))
			{
				LOG.info("URI:" + jObj.getString("uri"));
				reusableKey.set(jObj.getString("uri"));
				jOutput.accumulate("uri", jObj.getString("uri"));
				
			}
			
			if(jObj.containsKey("url"))
			{
				jOutput.accumulate("url", jObj.getString("url"));
				
			}
			
			context.write(reusableKey, new Text(jOutput.toString()));

		}
	}	
	
	public ArrayList<String> featureList()
	{
		ArrayList<String> featureList = new ArrayList<String>();
		
		featureList.add("person_age_feature");
		featureList.add("person_build_feature");
		featureList.add("person_bustbandsize_feature");
		featureList.add("person_cupsizeus_feature");
		featureList.add("person_ethnicity_feature");
		featureList.add("person_eyecolor_feature");
		featureList.add("person_gender_feature");
		featureList.add("person_grooming_feature");
		featureList.add("person_haircolor_feature");
		featureList.add("person_hairlength_feature");
		featureList.add("person_hairtype_feature");
		featureList.add("person_height_feature");
		featureList.add("person_hipstype_feature");
		featureList.add("person_implantspresent_feature");
		featureList.add("person_name_feature");
		featureList.add("person_piercings_feature");
		featureList.add("person_tattoocount_feature");
		featureList.add("person_username_feature");
		featureList.add("person_waistsize_feature");
		featureList.add("person_weight_feature");
		
		return featureList;
	}

}
