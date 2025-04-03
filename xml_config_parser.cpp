#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tinyxml2.h"
#include "xml_config_parser.h"
#include <sqlite3.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cjson/cJSON.h>
#include "mqtt_client.h"

using namespace tinyxml2;

// 检查并创建目录（导出为公共函数）
int ensure_directory(const char* path) {
    char dir[256] = {0};
    strncpy(dir, path, sizeof(dir) - 1);
    
    char* last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        
        // 递归创建目录
        char* p = dir;
        while (*p) {
            if (*p == '/') {
                *p = '\0';
                mkdir(dir, 0755);
                *p = '/';
            }
            p++;
        }
        mkdir(dir, 0755);
    }
    return 0;
}

// 全局配置对象
static SCL_Config g_sclConfig = {0};

// 导出全局变量
TopicMapping g_topicMapping = {0};

// 添加全局数据库连接对象
static sqlite3* db = NULL;

// 解析 FCDA 节点
static void parse_fcda_node(XMLElement* element, FCDA_Node *fcda) {
    const char* addr = element->Attribute("addr");
    const char* desc = element->Attribute("desc");
    const char* unit = element->Attribute("unit");
    
    if (addr) {
        fcda->addr = atoi(addr);
    }
    if (desc) {
        strncpy(fcda->desc, desc, sizeof(fcda->desc) - 1);
    }
    if (unit) {
        strncpy(fcda->unit, unit, sizeof(fcda->unit) - 1);
    }
}

// 解析 DataSet 节点
static void parse_dataset_node(XMLElement* element, DataSet_Node *dataset) {
    const char* dataName = element->Attribute("dataName");
    const char* desc = element->Attribute("desc");
    
    if (dataName) {
        strncpy(dataset->dataName, dataName, sizeof(dataset->dataName) - 1);
    }
    if (desc) {
        strncpy(dataset->desc, desc, sizeof(dataset->desc) - 1);
    }
    
    // 计算 FCDA 节点数量
    int fcdaCount = 0;
    for (XMLElement* child = element->FirstChildElement("FCDA"); 
         child; 
         child = child->NextSiblingElement("FCDA")) {
        fcdaCount++;
    }
    
    // 分配 FCDA 数组内存
    dataset->fcdaList = new FCDA_Node[fcdaCount]();
    dataset->fcdaCount = fcdaCount;
    
    // 解析 FCDA 节点
    int index = 0;
    for (XMLElement* child = element->FirstChildElement("FCDA"); 
         child; 
         child = child->NextSiblingElement("FCDA")) {
        parse_fcda_node(child, &dataset->fcdaList[index++]);
    }
}

// 解析 Device 节点
static void parse_device_node(XMLElement* element, Device_Node *device) {
    const char* deviceName = element->Attribute("deviceName");
    const char* desc = element->Attribute("desc");
    
    if (deviceName) {
        strncpy(device->deviceName, deviceName, sizeof(device->deviceName) - 1);
    }
    if (desc) {
        strncpy(device->desc, desc, sizeof(device->desc) - 1);
    }
    
    // 计算 DataSet 节点数量
    int dataSetCount = 0;
    for (XMLElement* child = element->FirstChildElement("DataSet"); 
         child; 
         child = child->NextSiblingElement("DataSet")) {
        dataSetCount++;
    }
    
    // 分配 DataSet 数组内存
    device->dataSetList = new DataSet_Node[dataSetCount]();
    device->dataSetCount = dataSetCount;
    
    // 解析 DataSet 节点
    int index = 0;
    for (XMLElement* child = element->FirstChildElement("DataSet"); 
         child; 
         child = child->NextSiblingElement("DataSet")) {
        parse_dataset_node(child, &device->dataSetList[index++]);
    }
}

// 解析 APP 节点
static void parse_app_node(XMLElement* element, APP_Node *app) {
    const char* appName = element->Attribute("appName");
    const char* desc = element->Attribute("desc");
    
    if (appName) {
        strncpy(app->appName, appName, sizeof(app->appName) - 1);
    }
    if (desc) {
        strncpy(app->desc, desc, sizeof(app->desc) - 1);
    }
    
    // 计算 Device 节点数量
    int deviceCount = 0;
    for (XMLElement* child = element->FirstChildElement("Device"); 
         child; 
         child = child->NextSiblingElement("Device")) {
        deviceCount++;
    }
    
    // 分配 Device 数组内存
    app->deviceList = new Device_Node[deviceCount]();
    app->deviceCount = deviceCount;
    
    // 解析 Device 节点
    int index = 0;
    for (XMLElement* child = element->FirstChildElement("Device"); 
         child; 
         child = child->NextSiblingElement("Device")) {
        parse_device_node(child, &app->deviceList[index++]);
    }
}

// 添加新的函数实现
void build_topic_mapping() {
    // 计算需要的映射总数
    int total_maps = 0;
    for (int i = 0; i < g_sclConfig.appCount; i++) {
        APP_Node *app = &g_sclConfig.appList[i];
        for (int j = 0; j < app->deviceCount; j++) {
            Device_Node *device = &app->deviceList[j];
            total_maps += device->dataSetCount;
        }
    }

    // 分配内存
    g_topicMapping.mapList = new TopicFCDA_Map[total_maps]();
    g_topicMapping.mapCount = total_maps;
    
    // 构建映射
    int map_index = 0;
    for (int i = 0; i < g_sclConfig.appCount; i++) {
        APP_Node *app = &g_sclConfig.appList[i];
        for (int j = 0; j < app->deviceCount; j++) {
            Device_Node *device = &app->deviceList[j];
            for (int k = 0; k < device->dataSetCount; k++) {
                DataSet_Node *dataset = &device->dataSetList[k];
                
                // 构建主题字符串
                snprintf(g_topicMapping.mapList[map_index].topic, 
                        sizeof(g_topicMapping.mapList[map_index].topic),
                        "%s/%s/%s/%s",
                        app->appName, "JSON", device->deviceName, dataset->dataName);
                
                //printf("创建主题映射: %s\n", g_topicMapping.mapList[map_index].topic);
                
                // 复制 FCDA 数据
                g_topicMapping.mapList[map_index].fcdaCount = dataset->fcdaCount;
                g_topicMapping.mapList[map_index].fcdaList = new FCDA_Node[dataset->fcdaCount];
                memcpy(g_topicMapping.mapList[map_index].fcdaList,
                       dataset->fcdaList,
                       sizeof(FCDA_Node) * dataset->fcdaCount);
                
                //printf("  FCDA 数量: %d\n", dataset->fcdaCount);
                map_index++;
            }
        }
    }
    
    printf("主题映射构建完成，映射数: %d\n", map_index);
}

void free_topic_mapping() {
    if (g_topicMapping.mapList) {
        for (int i = 0; i < g_topicMapping.mapCount; i++) {
            delete[] g_topicMapping.mapList[i].fcdaList;
        }
        delete[] g_topicMapping.mapList;
        g_topicMapping.mapList = NULL;
        g_topicMapping.mapCount = 0;
    }
}

const FCDA_Node* find_fcda_by_topic_addr(const char* topic, int addr) {
    for (int i = 0; i < g_topicMapping.mapCount; i++) {
        if (strcmp(g_topicMapping.mapList[i].topic, topic) == 0) {
            // 找到匹配的主题，遍历其 FCDA 列表
            for (int j = 0; j < g_topicMapping.mapList[i].fcdaCount; j++) {
                if (g_topicMapping.mapList[i].fcdaList[j].addr == addr) {
                    LOG_DEBUG("找到FCDA - topic: %s, addr: %d, desc: %s", 
                             topic, addr, g_topicMapping.mapList[i].fcdaList[j].desc);
                    return &g_topicMapping.mapList[i].fcdaList[j];
                }
            }
            LOG_WARN("未找到匹配的地址 - topic: %s, addr: %d", topic, addr);
            break;
        }
    }
    LOG_WARN("未找到匹配的主题: %s", topic);
    return NULL;
}

// 初始化数据库
int init_database(void) {
    const char* db_path = "sysdata/dataCenter.db";
    
    // 确保目录存在
    ensure_directory(db_path);
    
    int rc = sqlite3_open(db_path, &db);
    if (rc) {
        LOG_ERROR("无法打开数据库: %s", sqlite3_errmsg(db));
        return -1;
    }
    
    // 创建数据表
    const char* sql_create_table = 
        "CREATE TABLE IF NOT EXISTS sysData ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "topic TEXT NOT NULL,"
        "addr INTEGER NOT NULL,"
        "desc TEXT,"
        "val REAL,"
        "date TEXT NOT NULL,"
        "unit TEXT,"
        "qos INTEGER"
        ");";

    // 创建事件表
    const char* sql_create_event_table =
        "CREATE TABLE IF NOT EXISTS event ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "desc TEXT,"
        "date TEXT NOT NULL"
        ");";
    
    char* err_msg = 0;
    rc = sqlite3_exec(db, sql_create_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR("SQL错误: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }

    // 创建事件表
    rc = sqlite3_exec(db, sql_create_event_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        LOG_ERROR("创建事件表失败: %s", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }
    
    LOG_INFO("数据库初始化成功");
    return 0;
}

// 修改 store_mqtt_data 函数
int store_mqtt_data(const char* topic, int addr, const char* desc, 
                   double val, const char* date, const char* unit, int qos) {
    // 数据库存储逻辑
    if (!db) {
        LOG_ERROR("数据库未初始化");
        return -1;
    }
    
    sqlite3_stmt* stmt;
    const char* sql = "INSERT INTO sysData (topic, addr, desc, val, date, unit, qos) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?);";
    
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        LOG_ERROR("SQL准备失败: %s", sqlite3_errmsg(db));
        return -1;
    }
    
    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, addr);
    sqlite3_bind_text(stmt, 3, desc ? desc : "", -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 4, val);
    sqlite3_bind_text(stmt, 5, date, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 6, unit ? unit : "", -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 7, qos);
    
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        LOG_ERROR("插入数据失败: %s", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return -1;
    }
    
    sqlite3_finalize(stmt);
    LOG_DEBUG("数据已存储 - topic: %s, addr: %d, val: %.2f, time: %s", 
             topic, addr, val, date);
    return 0;
}

// 关闭数据库
void close_database() {
    if (db) {
        sqlite3_close(db);
        db = NULL;
        LOG_INFO("数据库已关闭");
    }
}

int query_and_upload_history(const char* topic, const char* start_time, 
                           const char* end_time) {
    if (!db) {
        LOG_ERROR("数据库未初始化");
        return -1;
    }

    sqlite3_stmt* stmt;
    const char* sql = "SELECT addr, val, date, desc, unit FROM sysData "
                     "WHERE topic = ? AND date BETWEEN ? AND ? "
                     "ORDER BY date ASC;";

    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        LOG_ERROR("SQL准备失败: %s", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, start_time, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, end_time, -1, SQLITE_STATIC);

    // 先计算总记录数
    int total_count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        total_count++;
    }
    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, topic, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, start_time, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, end_time, -1, SQLITE_STATIC);

    LOG_INFO("查询到 %d 条历史数据记录", total_count);

    // 如果没有数据，发送空结果消息
    if (total_count == 0) {
        cJSON *root = cJSON_CreateObject();
        cJSON *body = cJSON_CreateArray();
        
        cJSON_AddStringToObject(root, "type", "history");
        cJSON_AddStringToObject(root, "topic", topic);
        cJSON_AddStringToObject(root, "start_time", start_time);
        cJSON_AddStringToObject(root, "end_time", end_time);
        cJSON_AddNumberToObject(root, "count", 0);
        cJSON_AddNumberToObject(root, "total_count", 0);
        cJSON_AddNumberToObject(root, "batch", 0);
        cJSON_AddNumberToObject(root, "total_batches", 1);
        cJSON_AddItemToObject(root, "body", body);

        char *json_str = cJSON_Print(root);
        if (json_str) {
            LOG_INFO("发送空历史数据结果 - 主题: %s, 时间范围: %s 到 %s", 
                     topic, start_time, end_time);
            publish_message(json_str, topic);
            free(json_str);
        }
        
        cJSON_Delete(root);
        sqlite3_finalize(stmt);
        return 0;
    }

    const int BATCH_SIZE = 30;  // 一次最多发30条记录
    int batch_count = 0;
    int batch_number = 0;

    cJSON *root = NULL;
    cJSON *body = NULL;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        // 每批开始时创建新的JSON对象
        if (batch_count == 0) {
            root = cJSON_CreateObject();
            body = cJSON_CreateArray();
            if (!root || !body) {
                LOG_ERROR("创建JSON对象失败");
                sqlite3_finalize(stmt);
                return -1;
            }
            
            cJSON_AddStringToObject(root, "type", "history");
            cJSON_AddStringToObject(root, "topic", topic);
            cJSON_AddStringToObject(root, "start_time", start_time);
            cJSON_AddStringToObject(root, "end_time", end_time);
            cJSON_AddNumberToObject(root, "batch", batch_number);
            cJSON_AddItemToObject(root, "body", body);  // 提前添加body数组
        }

        // 添加记录到当前批次
        cJSON *item = cJSON_CreateObject();
        cJSON_AddStringToObject(item, "topic", topic);
        cJSON_AddNumberToObject(item, "addr", sqlite3_column_int(stmt, 0));
        cJSON_AddNumberToObject(item, "val", sqlite3_column_double(stmt, 1));
        cJSON_AddStringToObject(item, "date", (const char*)sqlite3_column_text(stmt, 2));
        cJSON_AddStringToObject(item, "desc", (const char*)sqlite3_column_text(stmt, 3));
        cJSON_AddStringToObject(item, "unit", (const char*)sqlite3_column_text(stmt, 4));
        cJSON_AddItemToArray(body, item);

        batch_count++;
        total_count++;

        // 检查当前JSON大小
        char *test_str = cJSON_Print(root);
        if (test_str) {
            size_t current_size = strlen(test_str);
            free(test_str);

            // 如果接近最大大小或达到批次大小，发送当前批次
            if (current_size > 4600 || batch_count == BATCH_SIZE) {
                cJSON_AddNumberToObject(root, "count", batch_count);

                char *json_str = cJSON_Print(root);
                if (json_str) {
                    LOG_INFO("发送历史数据批次 %d - 记录数: %d, 大小: %zu bytes", 
                            batch_number, batch_count, strlen(json_str));
                    publish_message(json_str, topic);
                    free(json_str);
                }

                cJSON_Delete(root);
                root = NULL;
                body = NULL;
                batch_count = 0;
                batch_number++;

                usleep(100000);  // 100ms延时
            }
        }
    }

    // 发送最后一批（如果有）
    if (batch_count > 0 && root != NULL) {
        cJSON_AddNumberToObject(root, "count", batch_count);

        char *json_str = cJSON_Print(root);
        if (json_str) {
            LOG_INFO("发送最后一批历史数据 - 批次: %d, 记录数: %d, 大小: %zu bytes", 
                    batch_number, batch_count, strlen(json_str));
            publish_message(json_str, topic);
            free(json_str);
        }

        cJSON_Delete(root);
    }

    sqlite3_finalize(stmt);
    LOG_INFO("历史数据发送完成 - 总记录数: %d, 总批次: %d", 
             total_count, batch_number + 1);
    return total_count;
}

// 修改事件存储函数
int store_event(const char* desc) {
    if (!db) {
        LOG_ERROR("数据库未初始化");
        return -1;
    }
    
    // 获取当前时间
    time_t now = time(NULL);
    struct tm *tm_now = localtime(&now);
    char date_str[32];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d %H:%M:%S", tm_now);
    
    sqlite3_stmt* stmt;
    const char* sql = "INSERT INTO event (desc, date) VALUES (?, ?);";
    
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        LOG_ERROR("SQL准备失败: %s", sqlite3_errmsg(db));
        return -1;
    }
    
    sqlite3_bind_text(stmt, 1, desc ? desc : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, date_str, -1, SQLITE_STATIC);
    
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        LOG_ERROR("插入事件失败: %s", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return -1;
    }
    
    sqlite3_finalize(stmt);
    LOG_INFO("事件已存储 - desc: %s, time: %s", desc, date_str);
    return 0;
}

// 查询并上传事件记录
int query_and_upload_events(const char* topic, const char* start_time, 
                          const char* end_time) {
    if (!db) {
        LOG_ERROR("数据库未初始化");
        return -1;
    }

    sqlite3_stmt* stmt;
    const char* sql = "SELECT desc, date FROM event "
                     "WHERE date BETWEEN ? AND ? "
                     "ORDER BY date ASC;";

    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        LOG_ERROR("SQL准备失败: %s", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, start_time, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, end_time, -1, SQLITE_STATIC);

    // 先计算总记录数
    int total_count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        total_count++;
    }
    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, start_time, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, end_time, -1, SQLITE_STATIC);

    LOG_INFO("查询到 %d 条事件记录", total_count);

    // 如果没有数据，发送空结果消息
    if (total_count == 0) {
        cJSON *root = cJSON_CreateObject();
        cJSON *body = cJSON_CreateArray();
        
        cJSON_AddStringToObject(root, "type", "event");
        cJSON_AddStringToObject(root, "start_time", start_time);
        cJSON_AddStringToObject(root, "end_time", end_time);
        cJSON_AddNumberToObject(root, "count", 0);
        cJSON_AddNumberToObject(root, "total_count", 0);
        cJSON_AddNumberToObject(root, "batch", 0);
        cJSON_AddNumberToObject(root, "total_batches", 1);
        cJSON_AddItemToObject(root, "body", body);

        char *json_str = cJSON_Print(root);
        if (json_str) {
            LOG_INFO("发送空事件记录结果 - 时间范围: %s 到 %s", 
                     start_time, end_time);
            publish_message(json_str, topic);
            free(json_str);
        }
        
        cJSON_Delete(root);
        sqlite3_finalize(stmt);
        return 0;
    }

    const int BATCH_SIZE = 30;  // 一次最多发30条记录
    int batch_count = 0;
    int batch_number = 0;

    cJSON *root = NULL;
    cJSON *body = NULL;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        // 每批开始时创建新的JSON对象
        if (batch_count == 0) {
            root = cJSON_CreateObject();
            body = cJSON_CreateArray();
            if (!root || !body) {
                LOG_ERROR("创建JSON对象失败");
                sqlite3_finalize(stmt);
                return -1;
            }
            
            cJSON_AddStringToObject(root, "type", "event");
            cJSON_AddStringToObject(root, "start_time", start_time);
            cJSON_AddStringToObject(root, "end_time", end_time);
            cJSON_AddNumberToObject(root, "batch", batch_number);
            cJSON_AddNumberToObject(root, "total_count", total_count);
            cJSON_AddItemToObject(root, "body", body);
        }

        // 添加记录到当前批次
        cJSON *item = cJSON_CreateObject();
        cJSON_AddStringToObject(item, "desc", (const char*)sqlite3_column_text(stmt, 0));
        cJSON_AddStringToObject(item, "date", (const char*)sqlite3_column_text(stmt, 1));
        cJSON_AddItemToArray(body, item);

        batch_count++;

        // 检查当前JSON大小
        char *test_str = cJSON_Print(root);
        if (test_str) {
            size_t current_size = strlen(test_str);
            free(test_str);

            // 如果接近最大大小或达到批次大小，发送当前批次
            if (current_size > 4600 || batch_count == BATCH_SIZE) {
                cJSON_AddNumberToObject(root, "count", batch_count);

                char *json_str = cJSON_Print(root);
                if (json_str) {
                    LOG_INFO("发送事件记录批次 %d - 记录数: %d, 大小: %zu bytes", 
                            batch_number, batch_count, strlen(json_str));
                    publish_message(json_str, topic);
                    free(json_str);
                }

                cJSON_Delete(root);
                root = NULL;
                body = NULL;
                batch_count = 0;
                batch_number++;

                usleep(100000);  // 100ms延时
            }
        }
    }

    // 发送最后一批（如果有）
    if (batch_count > 0 && root != NULL) {
        cJSON_AddNumberToObject(root, "count", batch_count);

        char *json_str = cJSON_Print(root);
        if (json_str) {
            LOG_INFO("发送最后一批事件记录 - 批次: %d, 记录数: %d, 大小: %zu bytes", 
                    batch_number, batch_count, strlen(json_str));
            publish_message(json_str, topic);
            free(json_str);
        }

        cJSON_Delete(root);
    }

    sqlite3_finalize(stmt);
    LOG_INFO("事件记录发送完成 - 总记录数: %d, 总批次: %d", 
             total_count, batch_number + 1);
    return total_count;
}

// 导出的 C 接口
extern "C" {

// 解析 XML 配置文件
int parse_xml_config(void) {
    const char* filename = "config/dataCenter.xml";
    
    // 确保配置目录存在
    ensure_directory(filename);

    XMLDocument doc;
    XMLError result = doc.LoadFile(filename);
    
    if (result != XML_SUCCESS) {
        LOG_ERROR("无法解析XML文件 %s", filename);
        return -1;
    }
    
    XMLElement* root = doc.RootElement();
    if (!root) {
        LOG_ERROR("空的XML文件");
        return -1;
    }
    
    // 检查根节点是否为 SCL
    if (strcmp(root->Name(), "SCL") != 0) {
        LOG_ERROR("根节点不是 SCL");
        return -1;
    }
    
    // 计算 APP 节点数量
    int appCount = 0;
    for (XMLElement* child = root->FirstChildElement("App"); 
         child; 
         child = child->NextSiblingElement("App")) {
        appCount++;
    }
    
    // 分配 APP 数组内存
    g_sclConfig.appList = new APP_Node[appCount]();
    g_sclConfig.appCount = appCount;
    
    // 解析 APP 节点
    int index = 0;
    for (XMLElement* child = root->FirstChildElement("App"); 
         child; 
         child = child->NextSiblingElement("App")) {
        parse_app_node(child, &g_sclConfig.appList[index++]);
    }
    
    // 构建主题映射 - 移除 result 判断，因为到这里说明解析成功
    build_topic_mapping();
    
    LOG_INFO("XML配置文件解析成功: %s", filename);
    return 0;  // 返回0表示成功
}

// 释放配置资源
void free_xml_config() {
    free_topic_mapping();  // 释放主题映射
    for (int i = 0; i < g_sclConfig.appCount; i++) {
        APP_Node *app = &g_sclConfig.appList[i];
        for (int j = 0; j < app->deviceCount; j++) {
            Device_Node *device = &app->deviceList[j];
            for (int k = 0; k < device->dataSetCount; k++) {
                DataSet_Node *dataset = &device->dataSetList[k];
                delete[] dataset->fcdaList;
            }
            delete[] device->dataSetList;
        }
        delete[] app->deviceList;
    }
    delete[] g_sclConfig.appList;
    memset(&g_sclConfig, 0, sizeof(SCL_Config));
}

// 打印配置信息（用于调试）
void print_xml_config() {
    printf("SCL Configuration:\n");
    for (int i = 0; i < g_sclConfig.appCount; i++) {
        APP_Node *app = &g_sclConfig.appList[i];
        printf("APP: %s (desc: %s)\n", app->appName, app->desc);
        
        for (int j = 0; j < app->deviceCount; j++) {
            Device_Node *device = &app->deviceList[j];
            printf("  Device: %s (desc: %s)\n", device->deviceName, device->desc);
            
            for (int k = 0; k < device->dataSetCount; k++) {
                DataSet_Node *dataset = &device->dataSetList[k];
                printf("    DataSet: %s (desc: %s)\n", dataset->dataName, dataset->desc);
                
                for (int l = 0; l < dataset->fcdaCount; l++) {
                    FCDA_Node *fcda = &dataset->fcdaList[l];
                    printf("      FCDA: addr=%d, desc=%s, unit=%s\n",
                           fcda->addr, fcda->desc, fcda->unit);
                }
            }
        }
    }
}

} // extern "C" 