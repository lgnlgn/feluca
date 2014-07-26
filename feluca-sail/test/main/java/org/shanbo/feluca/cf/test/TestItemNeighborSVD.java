package org.shanbo.feluca.cf.test;

import java.util.Properties;


import org.shanbo.feluca.cf.common.Evaluation;
import org.shanbo.feluca.cf.common.Recommender;
import org.shanbo.feluca.cf.stars.factorization.ItemNeighborModel;
import org.shanbo.feluca.cf.stars.factorization.RSVDModel;
import org.shanbo.feluca.cf.stars.factorization.SVDModel;
import org.shanbo.feluca.data.DataEntry;
import org.shanbo.feluca.data.DataEntry.RADataEntry;

public class TestItemNeighborSVD {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		String train = "data/movielens_train";
		String test = "data/movielens_test";
		
		RADataEntry traind = new RADataEntry(train);
		
		Recommender model = new ItemNeighborModel();
		
		Properties p = new Properties();
		p.setProperty("alpha", "0.004");
		p.setProperty("lambda", "0.02");
		p.setProperty("loops", "10");
		p.setProperty("factor", "30");
		p.setProperty("convergence", "0.90");
		p.setProperty("-i", "true");
		model.setProperties(p);
		model.loadData(traind);
		model.train();
		
		
		DataEntry testd = new DataEntry(test, false);
		
		
		
		double result = Evaluation.runRMSE(traind, testd, model);
		
		System.out.println("\n----\nRMSE:\t" + result);
		
	}

}
