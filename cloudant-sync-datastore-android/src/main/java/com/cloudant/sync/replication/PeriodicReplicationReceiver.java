/*
 * Copyright © 2016 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.replication;

import android.content.Context;
import android.content.Intent;
import android.support.v4.content.WakefulBroadcastReceiver;

/**
 * <p>This class implements a {@link WakefulBroadcastReceiver} that handles events related to
 * periodic replication.  It responds to reboot of the device to trigger the resetting of
 * the {@link android.app.AlarmManager} used to trigger periodic replication, and it
 * handles the periodic alarms sent by the {@link android.app.AlarmManager} to trigger
 * the periodic replications.</p>
 *
 * <p>The resetting of periodic alarms after reboot and the handling of the periodic alarms
 * are delegated to the {@link PeriodicReplicationService} associated with this
 * {@link android.content.BroadcastReceiver}.</p>
 *
 * @param <T> The {@link PeriodicReplicationService} component triggered by this
 * {@link android.content.BroadcastReceiver}
 *
 * @api_public
 */
public class PeriodicReplicationReceiver<T extends PeriodicReplicationService> extends WakefulBroadcastReceiver {

    static final String ALARM_ACTION = "com.cloudant.sync.replication.PeriodicReplicationReceiver.Alarm";

    Class<T> clazz;

    protected PeriodicReplicationReceiver(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent != null) {
            int command = PeriodicReplicationService.COMMAND_NONE;
            if (ALARM_ACTION.equals(intent.getAction())) {
                command = PeriodicReplicationService.COMMAND_START_REPLICATION;
            } else if (Intent.ACTION_BOOT_COMPLETED.equals(intent.getAction())) {
                command = PeriodicReplicationService.COMMAND_DEVICE_REBOOTED;
            }

            if (command != PeriodicReplicationService.COMMAND_NONE) {
                Intent serviceIntent = new Intent(context.getApplicationContext(), clazz);
                serviceIntent.putExtra(PeriodicReplicationService.EXTRA_COMMAND, command);
                startWakefulService(context, serviceIntent);
            }
        }
    }
}
