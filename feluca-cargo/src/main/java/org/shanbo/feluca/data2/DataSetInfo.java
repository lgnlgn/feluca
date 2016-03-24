package org.shanbo.feluca.data2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.shanbo.feluca.data2.Vector.VectorType;

public class DataSetInfo {

	List<Statistic> stats ;
	VectorType outVectorType;
	
	public static abstract class Statistic{
		protected Vector current;
		//basic
		public final static String NUM_VECTORS = "numVectors";
		public final static String MAX_FEATURE_ID = "maxFeatureId";
		public final static String TOTAL_FEATURES = "totalFeatures";
		public final static String MAX_VECTORSIZE = "maxVectorSize";

		//with weight
		public final static String SUM_WEIGHTS = "sumWeights";
		public final static String SUM_VEC_VALUE = "sumVecValue";
		public final static String LABEL_INFO = "labelInfo";
		public final static String CLASSES =  "classes";
		//with id
		public final static String MAX_VECTOR_ID = "maxVectorId";

		
		public void setCurrent(Vector v){
			this.current = v;
		}
		public abstract void statAsOne();
		public abstract void statOnFeature(int index);
		public abstract Properties getStatInfo();
	}
	
	public void setOutVectorType(VectorType vt){
		this.outVectorType = vt;
	}
	
	public void addStat(Statistic... stats){
		for(Statistic s : stats){
			this.stats.add(s);
		}
	}
	public void addStat(List<Statistic> stats){
		for(Statistic s : stats){
			this.stats.add(s);
		}
	}
	
	public Properties getStatInfo(){
		Properties p = new Properties();
		if (this.outVectorType != null){
			p.put("vectorType", outVectorType);
		}
		for(Statistic s : stats){
			p.putAll(s.getStatInfo());
		}
		return p;
	}
	
	public void doStat(Vector v){
		if (stats == null){
			stats = v.getStat();
			outVectorType = v.outputType;
		}
		for(Statistic st : stats){
			st.setCurrent(v);
			st.statAsOne();
		}
		for(int i = 0 ; i < v.getSize(); i++){
			for(Statistic st : stats){
				st.statOnFeature(i);
			}
		}
	}
	
	public String toString(){
		Properties p = getStatInfo();
		
		//we don't use properties's toString() or store()
		StringBuilder builder = new StringBuilder();
		for(Entry<Object, Object> entry : p.entrySet()){
			builder.append(entry.getKey() + "=" + entry.getValue() + "\n");
		}
		return builder.toString();
	}

	public static String prop2String(Properties p){
		StringBuilder builder = new StringBuilder();
		for(Entry<Object, Object> entry : p.entrySet()){
			builder.append(entry.getKey() + "=" + entry.getValue() + "\n");
		}
		return builder.toString();
	}
	
	public static Properties load(File f) throws IOException{
		Properties stat = new Properties();
		FileInputStream fis = new FileInputStream(f); 
		stat.load(fis);
		fis.close();
		return stat;
	}
	
}
