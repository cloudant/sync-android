/*
 * Copyright © 2016 Cloudant, Inc. All rights reserved.
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

package com.cloudant.todo.ui.dialogs;

import android.app.DialogFragment;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;

import com.cloudant.todo.R;

public class TaskDialog extends DialogFragment {

    private TaskCreatedListener mListener;
    private EditText mTaskDescription;

    public void setListener(TaskCreatedListener listener) {
        mListener = listener;
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle
        savedInstanceState) {
        View view = inflater.inflate(R.layout.dialog_new_task, container);
        mTaskDescription = (EditText) view.findViewById(R.id.new_task_desc);
        Button cancelButton = (Button) view.findViewById(R.id.btnCancel);
        final Button createButton = (Button) view.findViewById(R.id.btnCreate);
        getDialog().setTitle(R.string.new_task);

        cancelButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                dismiss();
            }
        });

        createButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (mListener != null) {
                    mListener.taskCreated(mTaskDescription.getText().toString());
                }
                mTaskDescription.getText().clear();
                dismiss();
            }
        });

        final TextWatcher textWatcher = new TextWatcher() {
            @Override
            public void onTextChanged(CharSequence s, int start,
                                      int before, int count) {
                createButton.setEnabled(mTaskDescription.getText().length() > 0);
            }

            @Override
            public void beforeTextChanged(CharSequence s, int start,
                                          int count, int after) {
            }

            @Override
            public void afterTextChanged(Editable s) {
            }
        };

        mTaskDescription.addTextChangedListener(textWatcher);

        return view;
    }

    public interface TaskCreatedListener {
        void taskCreated(String description);
    }
}
