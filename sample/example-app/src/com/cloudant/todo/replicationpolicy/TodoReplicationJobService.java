package com.cloudant.todo.replicationpolicy;

import com.cloudant.sync.replication.ReplicationJobService;

/**
 * Created by bryn on 02/04/2017.
 */

public class TodoReplicationJobService extends ReplicationJobService {
    public TodoReplicationJobService() {
        super(TodoReplicationService.class);
    }
}
