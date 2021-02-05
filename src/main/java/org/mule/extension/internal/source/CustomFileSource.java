package org.mule.extension.internal.source;

import static org.mule.runtime.extension.api.annotation.param.MediaType.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.mule.extension.internal.BasicConfiguration;
import org.mule.extension.internal.connection.BasicConnection;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.*;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.annotation.source.EmitsResponse;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.PollContext;
import org.mule.runtime.extension.api.runtime.source.PollingSource;
import org.mule.runtime.extension.api.runtime.source.SourceCallbackContext;


import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.mule.runtime.api.meta.model.display.PathModel.Location.EXTERNAL;
import static org.mule.runtime.api.meta.model.display.PathModel.Type.FILE;

@Alias("Get-Vehicle-Groups")
@EmitsResponse
@MediaType(value = ANY, strict = false)
public class CustomFileSource extends PollingSource<String, Void> {



    @Parameter
    @DisplayName("File Path")
    @Summary("File path")
    @org.mule.runtime.extension.api.annotation.param.display.Path(type = FILE, location = EXTERNAL)
    private String filePath;

    /**
     * A parameter that is not required to be configured by the user.
     */
    @DisplayName("Group Size")
    @Parameter
    @Optional(defaultValue = "10000")
    private int groupSize;


    @Override
    protected void doStart() throws MuleException {
    }

    @Override
    protected void doStop() {

    }

    @Override
    public void poll(PollContext<String, Void> pollContext) {

        if (pollContext.isSourceStopping()) {
            return;
        }
        int i = 0;
        String line = "";
        String previousLine = "";
        // ************ Iterate the file and group data.

        LineIterator it = null;
        StringBuilder responseStrBuilder = new StringBuilder();
        try {
            it = FileUtils.lineIterator(new File(filePath), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            while (it.hasNext()) {
                line = it.nextLine();
                if (line.startsWith("DH") || line.startsWith("FD")) //Ignore lines starting with DH and FD
                    continue;
                else if (line.startsWith("FC")) {
                    String s[] = line.split("\t");  //Extract the Factory_Accessory_Codes
                    if (previousLine.startsWith("FC")) {
                        responseStrBuilder.append("|");
                    }
                    responseStrBuilder.append(s[3]);
                    previousLine = "FC";
                    continue;
                } else if (line.startsWith("VI")) {
                    if (previousLine.startsWith("FC") || previousLine.startsWith("VI")) {
                        i++;
                        if (i % groupSize == 0) {
                            // Send the message to Mule flow.
                            pollContext.accept(item -> {
                                Result<String, Void> result = Result.<String, Void>builder()
                                        .output(responseStrBuilder.toString())
                                        .mediaType(org.mule.runtime.api.metadata.MediaType.TEXT)
                                        .build();
                                item.setResult(result);
                            });
                            responseStrBuilder.setLength(0);
                            responseStrBuilder.append(line);
                        } else {
                            responseStrBuilder.append("\n");
                            responseStrBuilder.append(line);
                        }
                    } else {

                        responseStrBuilder.append(line);
                    }
                    previousLine = "VI";
                }


            }
            // dispatch final vehicle record
            if (responseStrBuilder.length() > 0) {
                pollContext.accept(item -> {
                    Result<String, Void> result = Result.<String, Void>builder()
                            .output(responseStrBuilder.toString())
                            .mediaType(org.mule.runtime.api.metadata.MediaType.TEXT)
                            .build();
                    item.setResult(result);
                });
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        // Delete the temp file at the end.
        try {
            FileUtils.forceDelete(new File(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // ******************


    }

    @Override
    public void onRejectedItem(Result<String, Void> result, SourceCallbackContext callbackContext) {
        // ...
    }


}
