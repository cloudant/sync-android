package com.cloudant.todo;

import com.cloudant.sync.util.Document;

public class Task extends Document {
	
	// Default constructor is required for Document subclasses
	public Task() {}
	
	public Task(String desc) {
		this.setDescription(desc);
		this.setCompleted(false);
	}
	
	static final String DOC_TYPE = "com.cloudant.sync.example.task";
	private String type = DOC_TYPE;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	private boolean completed;
	public boolean isCompleted() {
		return this.completed;
	}
	public void setCompleted(boolean completed) {
		this.completed = completed;
	}
	
	private String description;
	public String getDescription() {
		return this.description;
	}
	public void setDescription(String desc) {
		this.description = desc;
	}
	
	@Override
	public String toString() {
		return "{ desc: " + getDescription() + ", completed: " + isCompleted() + "}";
	} 
}
