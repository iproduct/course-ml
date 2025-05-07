package org.iproduct.ksdemo;

import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.elements.exception.ConnectorException;

import java.io.IOException;

public class CoapSimpleClient {

    public static void main(String[] args) {

        CoapClient client = new CoapClient("coap://192.168.1.100:5683/time");

        CoapResponse response = null;
        try {
            response = client.get();
            if (response != null) {
                System.out.println(response.getCode());
                System.out.println(response.getOptions());
                System.out.println(response.getResponseText());
            } else {
                System.out.println("Request failed");
            }
        } catch (ConnectorException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

