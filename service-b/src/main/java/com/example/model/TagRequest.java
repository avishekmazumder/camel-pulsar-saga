
package com.example.model;

public class TagRequest {
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
