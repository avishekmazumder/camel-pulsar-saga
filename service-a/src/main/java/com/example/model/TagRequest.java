
package com.example.model;

import java.io.Serializable;

public class TagRequest implements Serializable {
    public TagRequest(String tag) {
        this.tag = tag;
    }

    public TagRequest() {
    }

    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Override
    public String toString() {
        return "TagRequest{" +
                "tag='" + tag + '\'' +
                '}';
    }
}
