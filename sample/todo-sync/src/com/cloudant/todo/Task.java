package com.cloudant.todo;

import com.cloudant.sync.datastore.DocumentRevision;

import java.util.HashMap;
import java.util.Map;

/*
 * Object representing a task.
 *
 * As well as acting as a value object, this class also has a reference to the original
 * DocumentRevision, which will be valid if the Task was fetched from the database, or else null
 * (eg for Tasks which have been created but not yet saved to the database).
 *
 * fromRevision() and asMap() act as helpers to map to and from JSON - in a real application
 * something more complex like an object mapper might be used.
 */

public class Task {

    private Task() {}

    public Task(String desc) {
        this.setDescription(desc);
        this.setCompleted(false);
        this.setType(DOC_TYPE);
    }

    // this is the revision in the database representing this task
    private DocumentRevision rev;
    public DocumentRevision getDocumentRevision() {
        return rev;
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

    public static Task fromRevision(DocumentRevision rev) {
        Task t = new Task();
        t.rev = rev;
        // this could also be done by a fancy object mapper
        Map<String, Object> map = rev.asMap();
        if(map.containsKey("type") && map.get("type").equals(Task.DOC_TYPE)) {
            t.setType((String) map.get("type"));
            t.setCompleted((Boolean) map.get("completed"));
            t.setDescription((String) map.get("description"));
            return t;
        }
        return null;
    }

    public Map<String, Object> asMap() {
        // this could also be done by a fancy object mapper
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("type", type);
        map.put("completed", completed);
        map.put("description", description);
        return map;
    }

}
