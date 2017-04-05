package com.cloudant.todo.replicationpolicy;

import com.cloudant.sync.replication.ReplicationJobService;

/**
 * Created by bryn on 02/04/2017.
 */

public class TwitterReplicationJobService extends ReplicationJobService {
    public TwitterReplicationJobService() {
        super(TwitterReplicationService.class);
    }
}
