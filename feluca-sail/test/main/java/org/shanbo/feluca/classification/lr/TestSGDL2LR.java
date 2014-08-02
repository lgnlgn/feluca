package org.shanbo.feluca.classification.lr;

import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.data2.DataEntry;

public class TestSGDL2LR {
	public static void main(String[] args) throws  Exception {
		SGDL1LR lr = new SGDL1LR();
		Properties p = new Properties();
		p.setProperty("alpha", "0.3");
		p.setProperty("lambda", "0.1");
		p.setProperty("loops", "30");
		lr.setProperties(p);
		lr.loadData(DataEntry.createDataEntry("data/real-sim", true));
		
//		lr.crossValidation(5, new Evaluator.BinaryAccuracy());
		System.out.println(lr.toString());
		lr.train();
		lr.saveModel("model/real-sim.model");
		System.out.println(lr.toString());
	}
}
