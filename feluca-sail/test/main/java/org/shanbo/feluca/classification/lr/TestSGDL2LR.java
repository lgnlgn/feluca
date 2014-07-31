package org.shanbo.feluca.classification.lr;

import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.data2.DataEntry;

public class TestSGDL2LR {
	public static void main(String[] args) throws  Exception {
		SGDL2LR lr = new SGDL2LR();
		Properties p = new Properties();
		p.setProperty("alpha", "0.5");
		p.setProperty("lambda", "0.00001");
		p.setProperty("loops", "30");
		lr.setProperties(p);
		lr.loadData(DataEntry.createDataEntry("data/real-sim", false));
		
//		lr.crossValidation(5, new Evaluator.BinaryAccuracy());
		System.out.println(lr.toString());
		lr.train();
		lr.saveModel("model/real-sim.model");
		System.out.println(lr.toString());
	}
}
