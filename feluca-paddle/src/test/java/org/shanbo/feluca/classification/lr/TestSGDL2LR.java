package org.shanbo.feluca.classification.lr;

import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.data.DataEntry;

public class TestSGDL2LR {
	public static void main(String[] args) throws  Exception {
		SGDL2LR lr = new SGDL2LR();
		Properties p = new Properties();
		p.setProperty("alpha", "0.4");
		p.setProperty("lambda", "0.3");
		lr.setProperties(p);
		lr.loadData(new DataEntry("data/covtype", false));
		
		lr.crossValidation(5, new Evaluator.BinaryAccuracy());
//		lr.train();
//		lr.saveModel("model/covtype.model");
	}
}
