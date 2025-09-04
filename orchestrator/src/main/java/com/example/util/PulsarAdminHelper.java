package com.example.util;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PulsarAdminHelper {

    private final PulsarAdmin admin;

    public PulsarAdminHelper(@Value("${app.pulsar.admin-url}") String adminUrl) throws Exception {
        this.admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
    }

    public void createTempSubscriptionLatest(String topic, String subscription) throws Exception {
        try {
            if (!admin.topics().getSubscriptions(topic).contains(subscription)) {
                admin.topics().createSubscription(topic, subscription, MessageId.latest);
            }
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.ConflictException ignored) {
            // already exists
        }
    }

    public void deleteSubscriptionQuiet(String topic, String subscription) {
        try {
            admin.topics().deleteSubscription(topic, subscription, false);
        } catch (Exception ignored) {}
    }
}
