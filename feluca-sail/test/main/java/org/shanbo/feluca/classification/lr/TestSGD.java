package org.shanbo.feluca.classification.lr;

import java.util.Properties;

import org.shanbo.feluca.classification.fmc.SGDFactorizeMachine;
import org.shanbo.feluca.data2.DataEntry;

public class TestSGD {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub



		AbstractSGDLogisticRegression lr = new SGDFactorizeMachine();
		
		Properties p = new Properties();
		p.setProperty("alpha", "0.4");
		p.setProperty("lambda", "0.005");
		p.setProperty("loops", "20");
		lr.setProperties(p);
		lr.loadData(DataEntry.createDataEntry("/home/lgn/data/realsim", true));

		//		lr.crossValidation(5, new Evaluator.BinaryAccuracy());
		System.out.println(lr.toString());
		lr.train();
	}

}
