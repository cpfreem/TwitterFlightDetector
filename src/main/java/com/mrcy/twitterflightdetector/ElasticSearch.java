package com.mrcy.twitterflightdetector;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;

/**
 *
 * @author cpfreem
 */
public class ElasticSearch {

    public static void main(String[] args) {
        // on startup
        Node node = nodeBuilder().node();
        Client client = node.client();

        // on shutdown
        node.close();

    }
}
