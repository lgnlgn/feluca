package org.shanbo.feluca.distribute.model.horizon;

import org.shanbo.feluca.common.Server;


public class SynModelServer extends Server{

	SyncModelLocal modelLocal;
	
	@Override
	public String serverName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int defaultPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String zkPathRegisterTo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void preStart() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void postStop() throws Exception {
		// TODO Auto-generated method stub
		
	}

	public SyncModelLocal getSynModel(){
		return modelLocal;
	}
	
}
