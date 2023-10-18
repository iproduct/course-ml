#include <WiFi.h>
#include <HTTPClient.h>

#define SSID "robots"
#define PASS "robot123"

char apiUrl[] = "http://192.168.1.100:3000/api/events";

HTTPClient http;


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
}

void loop() {
  Serial.println("[HTTP] begin ...");
  http.begin(apiUrl);
  Serial.println("[HTTP] GET");
  int status = http.GET();
  if (status > 0 && status < 400) {
    Serial.printf("Status: %d\n", status);
    String response = http.getString();
    Serial.println(response);
  } else {
    Serial.printf("HTTP Error: %d\n", status);
  }
  delay(5000);
}
