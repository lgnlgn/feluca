package org.shanbo.feluca.classification.lr;

import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.classification.fmc.SGDFactorizeMachine;
import org.shanbo.feluca.data2.DataEntry;

public class TestFMC {

	public static void main(String[] args) throws IOException, Exception {
		AbstractSGDLogisticRegression lr = new SGDL2LR();
		Properties p = new Properties();
		p.setProperty("alpha", "0.7");
		p.setProperty("lambda", "0.01");
		p.setProperty("loops", "16");
		p.setProperty("w0type", "0");
		p.setProperty("-w1", "1");
		lr.setProperties(p);
		
		lr.loadData(DataEntry.createDataEntry("/home/lgn/data/realsim", true));
		System.out.println(lr.toString());
		lr.train();
		

	}

}
