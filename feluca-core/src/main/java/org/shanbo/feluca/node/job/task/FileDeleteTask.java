package org.shanbo.feluca.node.job.task;

import java.io.File;
import java.util.List;

import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.node.job.TaskExecutor;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * delete files 
 * @author lgn
 *
 */
public class FileDeleteTask extends TaskExecutor{

	JSONObject message;
	List<String> toDeleteFiles;
	String repo; 
	boolean stop = false;
	public FileDeleteTask(JSONObject conf) {
		super(conf);
	}

	@Override
	public String getTaskFinalMessage() {
		return message.toJSONString();
	}

	@Override
	protected void init(JSONObject initConf) {
		JSONArray toDel =  initConf.getJSONObject("param").getJSONArray("files");
		toDeleteFiles = JSONUtil.JSONArrayToList(toDel);
		repo = initConf.getJSONObject("param").getString("repo");
		message = new JSONObject();
	}

	@Override
	public String getTaskName() {
		return "filedelete";
	}

	@Override
	protected void _exec() {	
		state = JobState.RUNNING;
		for(int i = 0 ; i < toDeleteFiles.size();i += 1){
			if (stop == true){
				break;
			}
			File toDel = new File((repo + "/" + toDeleteFiles.get(i)).replace("//", "/"));
			if (!toDel.exists()){
				message.put(toDeleteFiles.get(i), "not exist");
			}else{
				FileUtil.deleteFile(toDel);
				message.put(toDeleteFiles.get(i), "deleted");
			}
		}
		if (stop == true){
			state = JobState.INTERRUPTED;
		}else{
			state = JobState.FINISHED;
		}
	}

	@Override
	public void kill() {
		stop = true;
	}

}
