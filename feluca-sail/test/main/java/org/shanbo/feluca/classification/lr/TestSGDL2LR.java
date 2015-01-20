package org.shanbo.feluca.classification.lr;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.paddle.common.Utilities;

public class TestSGDL2LR {
	
	public static void testTrain(String model) throws Exception{
		AbstractSGDLogisticRegression lr = new SGDL2LR();
		Properties p = new Properties();
		p.setProperty("alpha", "1.2");
		p.setProperty("lambda", "0.5");
		p.setProperty("loops", "15");
		p.setProperty("-w1", "5");
		lr.setProperties(p);
		lr.loadData(DataEntry.createDataEntry("/home/lgn/data/avazutrain33", false));
		
//		lr.crossValidation(4, new Evaluator.BinaryAccuracy());
		System.out.println(lr.toString());
		lr.train();
		lr.saveModel(model);
		System.out.println(lr.toString());
	}
	
	public static void testTest(String model,String predict) throws Exception{
		SGDL2LR lr = new SGDL2LR();
		Properties p = new Properties();
		p.load(new FileReader("/home/lgn/data/avazutrain33/avazutrain33.sta"));
		lr.loadModel(model, p);
		DataEntry testSet = DataEntry.createDataEntry("/home/lgn/data/avazutest33", false);
		lr.predict(testSet, predict, new Evaluator.BinaryAccuracy());
	}
	
	public static void main(String[] args) throws  Exception {
		String model = "/home/lgn/kaggle/avazu.model";
		String predict = "/home/lgn/kaggle/avazu.predict";
		testTrain(model);
		System.out.println("===============================");
		testTest(model, predict);
	}
}
