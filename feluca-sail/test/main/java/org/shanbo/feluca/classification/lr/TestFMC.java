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
		p.setProperty("lambda", "0.001");
		p.setProperty("loops", "26");
		p.setProperty("-w1", "1");
		lr.setProperties(p);
		lr.loadData(DataEntry.createDataEntry("/home/lgn/data/realsim", true));
		lr.train();
		

	}

}
