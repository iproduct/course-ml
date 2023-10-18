#include <WiFi.h>
#include <WiFiUdp.h>
#include <coap-simple.h>
#include <ArduinoJson.h>

#define SSID "robots"
#define PASS "robot123"
#define MAX_JSON_SIZE 1024

IPAddress remote_ip(192, 168, 1, 100);
const int remote_port = 5683;
WiFiUDP udp;
Coap coap(udp, MAX_JSON_SIZE);

// CoAP client response callback
void callback_response(CoapPacket &packet, IPAddress ip, int port) {
  Serial.println("[Coap Response got]");

  char p[packet.payloadlen + 1];
  memcpy(p, packet.payload, packet.payloadlen);
  p[packet.payloadlen] = NULL;

  Serial.println(p);
}

// CoAP server endpoint URL
void callback_commands(CoapPacket &packet, IPAddress ip, int port) {
  Serial.println("[Commands] received");

  // send response
  char p[packet.payloadlen + 1];
  memcpy(p, packet.payload, packet.payloadlen);
  p[packet.payloadlen] = NULL;

  char t[packet.tokenlen + 1];
  memcpy(t, packet.token, packet.tokenlen);
  t[packet.tokenlen] = NULL;

  String message(p);
  String token(t);

  Serial.println(message);
  Serial.println(token);
  Serial.println(ip);
  Serial.println(port);
}


void setup() {
  Serial.begin(115200);
  while (!Serial) {
    ;
  }

  WiFi.mode(WIFI_MODE_STA);
  WiFi.begin(SSID, PASS);
  Serial.println();

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println();
  Serial.print("Connected to ");
  Serial.println(SSID);
  Serial.println("IP Address: ");
  Serial.println(WiFi.localIP());
  Serial.print("Signal strength: ");
  Serial.println(WiFi.RSSI());

  Serial.println("Setup Callback Commands");
  coap.server(callback_commands, "commands");

  // client response callback.
  // this endpoint is single callback.
  Serial.println("Setup Response Callback");
  coap.response(callback_response);


  // start coap server/client
  coap.start();

  // test if coap server is running
  coap.get(remote_ip, 5683, "time");
  coap.get(remote_ip, 5683, "sensors");
}

void loop() {
    coap.loop();
}
