#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

// MQTT 配置
#define MQTT_HOST "localhost"
#define MQTT_PORT "1883"

// 函数声明
void publish_message(const char* message, const char* topic);

#ifdef __cplusplus
}
#endif

#endif // MQTT_CLIENT_H 