#ifndef XML_CONFIG_PARSER_H
#define XML_CONFIG_PARSER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "logger.h"

// 数据结构定义
typedef struct {
    int addr;
    char desc[100];
    char unit[20];
} FCDA_Node;

typedef struct {
    char dataName[50];
    char desc[100];
    FCDA_Node *fcdaList;
    int fcdaCount;
} DataSet_Node;

typedef struct {
    char deviceName[50];
    char desc[100];
    DataSet_Node *dataSetList;
    int dataSetCount;
} Device_Node;

typedef struct {
    char appName[50];
    char desc[100];
    Device_Node *deviceList;
    int deviceCount;
} APP_Node;

typedef struct {
    APP_Node *appList;
    int appCount;
} SCL_Config;

// 在现有结构体定义后添加
typedef struct {
    char topic[150];        // 存储格式: appName/deviceName/dataName
    FCDA_Node *fcdaList;    // 该主题下的所有 FCDA 节点
    int fcdaCount;          // FCDA 节点数量
} TopicFCDA_Map;

typedef struct {
    TopicFCDA_Map *mapList;
    int mapCount;
} TopicMapping;

// 导出全局变量
extern TopicMapping g_topicMapping;

// 函数声明
int parse_xml_config(void);
void free_xml_config(void);
void print_xml_config(void);
const FCDA_Node* find_fcda_by_topic_addr(const char* topic, int addr);
void build_topic_mapping(void);
void free_topic_mapping(void);

// 添加数据库相关函数声明
int init_database(void);
int store_mqtt_data(const char* topic, int addr, const char* desc, 
                   double val, const char* date, const char* unit, int qos);
int store_event(const char* desc);
int query_and_upload_history(const char* topic, const char* start_time, 
                            const char* end_time);
int query_and_upload_events(const char* topic, const char* start_time, 
                            const char* end_time);
void close_database(void);

// 工具函数
int ensure_directory(const char* path);

#ifdef __cplusplus
}
#endif

#endif // XML_CONFIG_PARSER_H 