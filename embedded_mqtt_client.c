#define _XOPEN_SOURCE
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <cjson/cJSON.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include "xml_config_parser.h"
#include "mqtt_client.h"

// 静态函数声明
static char* read_log_file(const char* filename, size_t* size, 
                          const char* start_str, const char* end_str);
static void send_log_content(const char* topic, const char* content, size_t size);
static void upload_log_file(const char* topic, const char* filename, 
                          const char* start_time, const char* end_time);
static void process_data(int topic_index, int addr_value, double val_value, const char* topic);

#define MQTT_HOST "localhost"
#define MQTT_PORT "1883"

// 修改主题数组定义
char mqtt_topic[100][50];  // 最多100个主题，每个主题最长49个字符
int topic_count = 0;

const size_t MAX_SEGMENT_SIZE = 5000;  // MQTT消息大小限制

// 添加数据结构定义
#define MAX_DATA_ITEMS 300  // 最大数据项数量

typedef struct {
    int addr;
    double val;
} MqttData;

typedef struct {
    char topic[50];
    MqttData data[MAX_DATA_ITEMS];
    int data_count;
    char timestamp[32];
} TopicData;

// 修改全局数据存储为指针，用于指向共享内存
TopicData *topic_storage = NULL;

// 添加静态变量用于跟踪上次存储的分钟数
static int last_store_minute = -1;  // 初始化为-1表示未存储过

// 添加定时存储函数
void store_periodic_data() {
    time_t now = time(NULL);
    struct tm *tm_now = localtime(&now);
    int current_minute = tm_now->tm_hour * 60 + tm_now->tm_min;

    // 检查是否需要存储
    if (current_minute % 5 != 0 || current_minute == last_store_minute) {
        return;
    }
    
    // 检查 topic_storage 是否已初始化
    if (!topic_storage) {
        LOG_ERROR("topic_storage 未初始化");
        return;
    }
    
    LOG_INFO("开始存储数据");
    store_event("执行数据定时存储");

    last_store_minute = current_minute;
    
    char date_str[32];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d %H:%M:00", tm_now);

    for (int i = 0; i < topic_count; i++) {
        TopicData *topic_data = &topic_storage[i];
        
        // 检查主题是否有效
        if (!topic_data->topic[0]) {
            LOG_WARN("主题 [%d] 为空", i);
            continue;
        }

        // 检查数据项数量是否有效
        if (topic_data->data_count <= 0 || topic_data->data_count > MAX_DATA_ITEMS) {
            //LOG_WARN("主题 [%s] 的数据项数量无效: %d", topic_data->topic, topic_data->data_count);
            continue;
        }
        
        //LOG_INFO("处理主题[%d]: %s", i, topic_data->topic);
        //LOG_INFO("数据项数量: %d", topic_data->data_count);
        
        for (int j = 0; j < topic_data->data_count; j++) {
            // 获取 FCDA 节点信息
            const FCDA_Node* fcda = find_fcda_by_topic_addr(topic_data->topic, 
                                                         topic_data->data[j].addr);
            if (!fcda) {
                LOG_WARN("未找到主题 [%s] 地址 [%d] 的 FCDA 节点",
                        topic_data->topic, topic_data->data[j].addr);
            }
            
            //LOG_INFO("  存储数据[%d]: addr=%d, val=%f, desc=%s, unit=%s",
            //       j, topic_data->data[j].addr, topic_data->data[j].val,
            //       fcda ? fcda->desc : "(无描述)",
            //       fcda ? fcda->unit : "(无单位)");
            
            store_mqtt_data(topic_data->topic, 
                         topic_data->data[j].addr,
                         fcda ? fcda->desc : "",
                         topic_data->data[j].val,
                         date_str,
                         fcda ? fcda->unit : "",
                         0);
        }
    }
    
    //LOG_INFO("完成数据存储 - 时间: %s", date_str);
}

// 初始化默认主题
void init_topics() {
    // 获取 TopicMapping 中的主题列表
    extern TopicMapping g_topicMapping;  // 声明外部变量
    
    // 初始化 topic_storage
    if (!topic_storage) {
        topic_storage = (TopicData *)calloc(100, sizeof(TopicData));
        if (!topic_storage) {
            LOG_ERROR("无法分配 topic_storage 内存");
            return;
        }
    }
    
    topic_count = 0;
    for (int i = 0; i < g_topicMapping.mapCount && i < 100; i++) {
        strncpy(mqtt_topic[topic_count], g_topicMapping.mapList[i].topic, 49);
        // 同时初始化 topic_storage 中的主题
        strncpy(topic_storage[topic_count].topic, g_topicMapping.mapList[i].topic, 49);
        topic_storage[topic_count].data_count = 0;  // 初始化数据计数
        LOG_INFO("添加主题[%d]: %s", topic_count, mqtt_topic[topic_count]);
        topic_count++;
    }
    
    LOG_INFO("总共加载了 %d 个主题", topic_count);
}

// JSON消息创建函数
char* create_json_message(const char* sensor_name, float value) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "sensor", sensor_name);
    cJSON_AddNumberToObject(root, "value", value);
    
    char *string = cJSON_Print(root);
    cJSON_Delete(root);
    return string;
}

// 修改 publish_message 函数
void publish_message(const char* message, const char* topic) {
    int max_mes_len = MAX_SEGMENT_SIZE + 1000;
    // 增加缓冲区大小以容纳大型消息
    // MAX_SEGMENT_SIZE(4000) + 命令和主题等额外文本(约200) + 安全边界(约800)
    char command[max_mes_len];

    // 检查消息长度是否超出缓冲区限制
    size_t msg_len = strlen(message);
    if (msg_len > max_mes_len) {
        LOG_ERROR("消息太大: %zu bytes", msg_len);
        return;
    }

    snprintf(command, sizeof(command), 
             "mosquitto_pub -h %s -p %s -t %s -m '%s'",
             MQTT_HOST, MQTT_PORT, topic, message);

    // 检查 snprintf 是否截断
    if (strlen(command) >= sizeof(command) - 1) {
        LOG_ERROR("命令字符串被截断");
        return;
    }

    int result = system(command);
    if (result != 0) {
        LOG_ERROR("发布消息失败: %s", strerror(errno));
    }
}

// 在 TopicData 结构体中添加查找函数
int find_data_by_addr(TopicData *storage, int addr) {
    for (int i = 0; i < storage->data_count; i++) {
        if (storage->data[i].addr == addr) {
            return i;  // 返回找到的索引
        }
    }
    return -1;  // 没找到返回-1
}

// MQTT数据处理和存储
void process_json_message(const char* json_string, const char* topic) {
    cJSON *root = cJSON_Parse(json_string);
    if (root == NULL) {
        LOG_ERROR("解析JSON失败");
        return;
    }

    // 检查消息类型
    cJSON *type = cJSON_GetObjectItem(root, "type");
    if (type && type->valuestring) {
        const char* type_str = type->valuestring;
        if (strcmp(type_str, "log") == 0 || 
            strcmp(type_str, "history") == 0 || 
            strcmp(type_str, "event") == 0) {
            LOG_DEBUG("收到特殊类型消息 [%s]，跳过处理", type_str);
            cJSON_Delete(root);
            return;
        }
    }

    LOG_INFO("收到主题 [%s] 的消息", topic);

    // 查找主题存储
    int topic_index = -1;
    for (int i = 0; i < topic_count; i++) {
        if (strcmp(topic, mqtt_topic[i]) == 0) {
            topic_index = i;
            break;
        }
    }
    
    if (topic_index == -1) {
        LOG_ERROR("未找到对应主题的存储空间");
        cJSON_Delete(root);
        return;
    }

    if (strstr(topic, "/YK") != NULL) { //遥控数据
        // 解析遥控数据特有字段
        cJSON *cot = cJSON_GetObjectItem(root, "cot");
        cJSON *rii = cJSON_GetObjectItem(root, "rii");
        cJSON *cmd = cJSON_GetObjectItem(root, "cmd");
        cJSON *addr = cJSON_GetObjectItem(root, "addr");
        cJSON *para = cJSON_GetObjectItem(root, "para");
        
        if (cot && rii && cmd && addr) {
            char event_desc[256];
            snprintf(event_desc, sizeof(event_desc), 
                    "遥控命令 - cot: %d, rii: %u, cmd: %d, addr: %d",
                    cot->valueint, 
                    (unsigned int)rii->valueint,  // 使用 valueint 并转换为无符号整数
                    cmd->valueint,
                    addr->valueint);
            
            // 记录遥控事件
            store_event(event_desc);
            LOG_INFO("%s", event_desc);
            
            // 如果有参数数组，记录参数
            if (para && cJSON_IsArray(para)) {
                int para_count = cJSON_GetArraySize(para);
                if (para_count > 0) {
                    char para_desc[256] = "遥控参数:";
                    for (int i = 0; i < para_count; i++) {
                        cJSON *para_item = cJSON_GetArrayItem(para, i);
                        if (para_item) {
                            char temp[32];
                            snprintf(temp, sizeof(temp), " %d", para_item->valueint);
                            strcat(para_desc, temp);
                        }
                    }
                    store_event(para_desc);
                    LOG_INFO("%s", para_desc);
                }
            }
        } else {
            LOG_ERROR("遥控数据格式错误 - 缺少必要字段");
        }
        cJSON_Delete(root);  // 添加这行确保释放内存
        return;
    }

    cJSON *timestamp = cJSON_GetObjectItem(root, "timestamp");
    if (timestamp && timestamp->valuestring) {
        strncpy(topic_storage[topic_index].timestamp, timestamp->valuestring, 
                sizeof(topic_storage[topic_index].timestamp) - 1);
    }

    // 处理消息体
    cJSON *body = cJSON_GetObjectItem(root, "body");
    if (body) {
        LOG_INFO("处理主题 [%s] 的数据", topic);
        
        int body_size = cJSON_GetArraySize(body);
        for (int i = 0; i < body_size && i < MAX_DATA_ITEMS; i++) {
            cJSON *item = cJSON_GetArrayItem(body, i);
            if (item) {
                cJSON *addr = cJSON_GetObjectItem(item, "addr");
                cJSON *val = cJSON_GetObjectItem(item, "val");

                if (addr && val) {
                    int addr_value = addr->valueint;
                    double val_value = val->valuedouble;
                    process_data(topic_index, addr_value, val_value, topic);

                    // dataCenter/JSON/*主题
                    if (strstr(topic, "dataCenter/JSON") == topic) {
                        switch (addr_value) {
                            case 0:
                                // 日志查询和上传
                                LOG_INFO("收到日志查询请求");
                                store_event("收到日志查询请求");
                                
                                // 获取开始时间和结束时间
                                cJSON *start_time_log = cJSON_GetObjectItem(item, "start_time");
                                cJSON *stop_time_log = cJSON_GetObjectItem(item, "stop_time");
                                
                                if (start_time_log && stop_time_log && 
                                    start_time_log->valuestring && stop_time_log->valuestring) {
                                    LOG_INFO("查询日志记录 - 开始时间: %s, 结束时间: %s",
                                            start_time_log->valuestring, stop_time_log->valuestring);

                                    // 上传当前日志文件
                                    upload_log_file("dataCenter/JSON/LD1/YX", "logs/dataCenter.log", 
                                                  start_time_log->valuestring, stop_time_log->valuestring);
                                    
                                    // 检查并上传历史日志文件
                                    if (access("logs/dataCenter.log.1", F_OK) == 0)
                                    {
                                        upload_log_file("dataCenter/JSON/LD1/YX", "logs/dataCenter.log.1", 
                                                      start_time_log->valuestring, stop_time_log->valuestring);
                                    }
                                    if (access("logs/dataCenter.log.2", F_OK) == 0)
                                    {
                                        upload_log_file("dataCenter/JSON/LD1/YX", "logs/dataCenter.log.2", 
                                                      start_time_log->valuestring, stop_time_log->valuestring);
                                    }
                                } else {
                                    LOG_ERROR("日志查询参数无效 - 缺少开始时间或结束时间");
                                }
                                break;
                            case 2:
                                // 历史数据查询和上传
                                LOG_INFO("收到历史数据查询请求");
                                store_event("收到历史数据查询请求");

                                cJSON *topicItem = cJSON_GetObjectItem(item, "topic");
                                if (!topicItem || !cJSON_IsString(topicItem)) {
                                    LOG_ERROR("历史数据查询缺少有效的 topic 字段");
                                    break;
                                }
                                const char *topic_histry = topicItem->valuestring;

                                // 获取开始时间和结束时间
                                cJSON *start_time_history = cJSON_GetObjectItem(item, "start_time");
                                cJSON *stop_time_history = cJSON_GetObjectItem(item, "stop_time");
                                
                                if (start_time_history && stop_time_history && 
                                    start_time_history->valuestring && stop_time_history->valuestring) {
                                    LOG_INFO("查询历史数据 - 开始时间: %s, 结束时间: %s",
                                            start_time_history->valuestring, stop_time_history->valuestring);
                                    
                                    // 查询并上传数据
                                    query_and_upload_history(topic_histry, start_time_history->valuestring, stop_time_history->valuestring);
                                } else {
                                    LOG_ERROR("历史数据查询参数无效");
                                }
                                break;
                            case 4:
                                // 事件查询和上传
                                LOG_INFO("收到事件查询请求");
                                store_event("收到事件查询请求");
 
                                // 获取开始时间和结束时间
                                cJSON *start_time_event = cJSON_GetObjectItem(item, "start_time");
                                cJSON *stop_time_event = cJSON_GetObjectItem(item, "stop_time");
                                
                                if (start_time_event && stop_time_event && 
                                    start_time_event->valuestring && stop_time_event->valuestring) {
                                    LOG_INFO("查询事件记录 - 开始时间: %s, 结束时间: %s",
                                            start_time_event->valuestring, stop_time_event->valuestring);
                                    
                                    // 调用事件查询函数
                                    query_and_upload_events(topic, start_time_event->valuestring, stop_time_event->valuestring);
                                }
                                break;
                            default:
                                LOG_WARN("未知的查询类型: addr=%d", addr_value);
                                break;
                        }
                    }
                }
            }
        }
    } else {
        LOG_WARN("消息中未找到 body 字段 - topic: %s", topic);
    }

    // 打印存储的数据摘要（按addr排序）
    //LOG_INFO("主题 [%s] 数据摘要:", topic);
    //LOG_INFO("时间戳: %s", topic_storage[topic_index].timestamp);
    //LOG_INFO("数据项数量: %d", topic_storage[topic_index].data_count);
    
    // 创建临时数组用于排序
    MqttData sorted_data[MAX_DATA_ITEMS];
    memcpy(sorted_data, topic_storage[topic_index].data, 
           sizeof(MqttData) * topic_storage[topic_index].data_count);
    
    // 按addr排序（简单冒泡排序）
    for (int i = 0; i < topic_storage[topic_index].data_count - 1; i++) {
        for (int j = 0; j < topic_storage[topic_index].data_count - i - 1; j++) {
            if (sorted_data[j].addr > sorted_data[j + 1].addr) {
                MqttData temp = sorted_data[j];
                sorted_data[j] = sorted_data[j + 1];
                sorted_data[j + 1] = temp;
            }
        }
    }
    
    // 打印排序后的数据
    // for (int i = 0; i < topic_storage[topic_index].data_count; i++) {
    //     LOG_INFO("数据项 %d - addr: %d, val: %f", 
    //            i, sorted_data[i].addr, sorted_data[i].val);
    // }

    cJSON_Delete(root);
}

// 添加获取数据的辅助函数
MqttData* get_topic_data(const char* topic, int* count) {
    for (int i = 0; i < topic_count; i++) {
        if (strcmp(topic, mqtt_topic[i]) == 0) {
            *count = topic_storage[i].data_count;
            return topic_storage[i].data;
        }
    }
    *count = 0;
    return NULL;
}

// 获取主题最新时间戳
const char* get_topic_timestamp(const char* topic) {
    for (int i = 0; i < topic_count; i++) {
        if (strcmp(topic, mqtt_topic[i]) == 0) {
            return topic_storage[i].timestamp;
        }
    }
    return NULL;
}

// 修改 subscribe_callback 函数来传递主题信息
void subscribe_callback(void) {
    // 构建订阅命令，订阅所有配置的主题
    char command[2048] = {0};
    snprintf(command, sizeof(command),
             "mosquitto_sub -v -h %s -p %s", MQTT_HOST, MQTT_PORT);  // 添加 -v 参数
    
    //LOG_INFO("订阅主题=");
    // 添加所有主题到订阅命令
    for (int i = 0; i < topic_count; i++) {
        char topic_arg[150];
        snprintf(topic_arg, sizeof(topic_arg), " -t %s", mqtt_topic[i]);
        strcat(command, topic_arg);
        //LOG_INFO("%s", mqtt_topic[i]);
    }

    FILE *pipe = popen(command, "r");
    if (!pipe) {
        LOG_ERROR("订阅失败");
        return;
    }

    char buffer[1024];
    char message[4096] = {0};
    char current_topic[100] = {0};
    size_t msg_len = 0;

    // 添加标志位，用于跟踪消息处理状态
    int processing_message = 0;

    while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
        size_t len = strlen(buffer);
        
        // mosquitto_sub 的输出格式为: topic_name message
        // 首先尝试提取主题
        if (!processing_message) {  // 新消息开始
            char *space = strchr(buffer, ' ');
            if (space) {
                size_t topic_len = space - buffer;
                if (topic_len < sizeof(current_topic)) {
                    strncpy(current_topic, buffer, topic_len);
                    current_topic[topic_len] = '\0';
                    
                    // 移动消息内容到缓冲区
                    len -= (topic_len + 1);  // +1 for space
                    memmove(buffer, space + 1, len);
                    buffer[len] = '\0';  // 确保字符串正确终止
                    processing_message = 1;  // 开始处理新消息
                    
                    // 清零并初始化消息缓冲区
                    memset(message, 0, sizeof(message));
                    msg_len = 0;
                }
            }
        }
        
        // 检查缓冲区是否足够
        if (len >= sizeof(message)) {  // 检查单条消息是否超出缓冲区
            LOG_INFO("消息太长，清空缓冲区");
            memset(message, 0, sizeof(message));
            msg_len = 0;
            processing_message = 0;  // 重置处理状态
            continue;
        }

        // 追加到消息缓冲区
        memcpy(message + msg_len, buffer, len);
        msg_len += len;

        // 检查是否是完整的JSON消息
        int brace_count = 0;
        int complete = 0;
        
        for (size_t i = 0; i < msg_len; i++) {
            if (message[i] == '{') brace_count++;
            if (message[i] == '}') brace_count--;
            
            if (brace_count == 0 && i > 0) {
                // 找到完整的JSON消息
                message[i + 1] = '\0';
                process_json_message(message, current_topic);
                
                // 重置消息处理状态
                memset(message, 0, sizeof(message));
                msg_len = 0;
                processing_message = 0;
                complete = 1;
                break;
            }
        }
        
        // 如果没有找到完整的JSON，继续读取
        if (!complete && msg_len > 0) {
            continue;
        }

        // 如果消息处理完成，重置所有状态
        if (complete) {
            memset(message, 0, sizeof(message));
            msg_len = 0;
            processing_message = 0;
        }
    }

    pclose(pipe);
}

int main(int argc, char **argv) {
    if (argc != 1) {
        LOG_ERROR("Usage: %s", argv[0]);
        return 1;
    }

    // 初始化日志系统
    if (init_logger("logs/dataCenter.log", LOGGER_DEBUG) != 0) {
        printf("初始化日志系统失败\n");
        return 1;
    }
    LOG_INFO("程序启动");

    // 创建共享内存
    int shm_size = sizeof(TopicData) * 100;  // 为100个主题分配空间
    int shm_fd = shm_open("/mqtt_topic_storage", O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        LOG_ERROR("共享内存创建失败: %s", strerror(errno));
        return 1;
    }
    
    if (ftruncate(shm_fd, shm_size) == -1) {
        perror("ftruncate failed");
        return 1;
    }
    
    //将文件映射为虚拟内存，便于频繁读写
    topic_storage = (TopicData *)mmap(NULL, shm_size, 
                                     PROT_READ | PROT_WRITE, 
                                     MAP_SHARED, shm_fd, 0);
    if (topic_storage == MAP_FAILED) {
        perror("mmap failed");
        return 1;
    }

    // 初始化数据库
    if (init_database() != 0) {
        printf("初始化数据库失败\n");
        return 1;
    }

    system("pkill mosquitto_sub");//结束之前的进程

    if (parse_xml_config() == 0) {
        //print_xml_config();
        //free_xml_config();
    }

    // 记录程序启动事件
    store_event("数据中心程序启动");

    // 初始化主题
    init_topics();

    // 创建子进程
    pid_t pid = fork();
    
    if (pid < 0) {
        printf("创建进程失败\n");
        return -1;
    }
    else if (pid == 0) {
        // 子进程：处理订阅
        subscribe_callback();
    }
    else {
        // 父进程：定时存储数据和发布消息
        while (1) {
            // 检查是否需要存储数据
            store_periodic_data();
            
            // 发布消息
            // for (int i = 0; i < topic_count; i++) {
            //     char *message = create_json_message("temperature", 25.5);
            //     publish_message(message, mqtt_topic[i]);
            //     free(message);
            // }
            
            sleep(1);  // 每秒检查一次
        }
    }

    // 清理共享内存
    munmap(topic_storage, shm_size);
    close(shm_fd);
    shm_unlink("/mqtt_topic_storage");

    // 在程序结束时关闭数据库
    close_database();

    // 在程序结束时关闭日志系统
    LOG_INFO("程序退出");
    close_logger();
    return 0;
}

// 添加新的辅助函数用于处理数据
static void process_data(int topic_index, int addr_value, double val_value, const char* topic) {
    // 查找是否已存在该addr的数据
    int existing_index = find_data_by_addr(&topic_storage[topic_index], addr_value);
    
    if (existing_index >= 0) {
        // 更新已存在的数据
        topic_storage[topic_index].data[existing_index].val = val_value;
        //LOG_DEBUG("更新数据 - topic: %s, addr: %d, val: %f", 
        //         topic, addr_value, val_value);
    } else if (topic_storage[topic_index].data_count < MAX_DATA_ITEMS) {
        // 添加新数据
        topic_storage[topic_index].data[topic_storage[topic_index].data_count].addr = addr_value;
        topic_storage[topic_index].data[topic_storage[topic_index].data_count].val = val_value;
        topic_storage[topic_index].data_count++;
        //LOG_DEBUG("添加新数据 - topic: %s, addr: %d, val: %f", 
        //         topic, addr_value, val_value);
    } else {
        LOG_WARN("数据项已达到最大限制 %d - topic: %s", 
                 MAX_DATA_ITEMS, topic);
    }
}

// 读取日志文件内容
static char* read_log_file(const char* filename, size_t* size, 
                          const char* start_str, const char* end_str) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        LOG_ERROR("无法打开日志文件: %s", filename);
        return NULL;
    }

    // 分配初始缓冲区
    size_t buffer_size = 4096;  // 初始缓冲区大小
    char* buffer = (char*)malloc(buffer_size);
    if (!buffer) {
        LOG_ERROR("内存分配失败");
        fclose(file);
        return NULL;
    }

    size_t total_size = 0;
    char line[1024];
    time_t start_time, end_time, log_time;

    // 转换时间字符串为时间戳
    struct tm tm_start = {0}, tm_end = {0};
    strptime(start_str, "%Y-%m-%d %H:%M:%S", &tm_start);
    strptime(end_str, "%Y-%m-%d %H:%M:%S", &tm_end);
    start_time = mktime(&tm_start);
    end_time = mktime(&tm_end);

    while (fgets(line, sizeof(line), file)) {
        // 解析日志行中的时间戳
        char time_str[32];
        if (sscanf(line, "[%[^]]", time_str) == 1) {
            struct tm tm_log = {0};
            strptime(time_str, "%Y-%m-%d %H:%M:%S", &tm_log);
            log_time = mktime(&tm_log);

            // 检查时间是否在范围内
            if (log_time >= start_time && log_time <= end_time) {
                size_t line_len = strlen(line);
                
                // 检查是否需要扩展缓冲区
                if (total_size + line_len + 1 > buffer_size) {
                    buffer_size *= 2;
                    char* new_buffer = (char*)realloc(buffer, buffer_size);
                    if (!new_buffer) {
                        LOG_ERROR("重新分配内存失败");
                        free(buffer);
                        fclose(file);
                        return NULL;
                    }
                    buffer = new_buffer;
                }

                // 添加行到缓冲区
                memcpy(buffer + total_size, line, line_len);
                total_size += line_len;
            }
        }
    }

    if (total_size == 0) {
        // 没有找到符合时间范围的日志
        free(buffer);
        fclose(file);
        *size = 0;
        return NULL;
    }

    // 添加字符串结束符
    buffer[total_size] = '\0';
    *size = total_size;

    fclose(file);
    return buffer;
}

// 发送日志文件内容
static void send_log_content(const char* topic, const char* content, size_t size) {
    LOG_INFO("开始发送日志内容 - 大小: %zu bytes", size);
    
    int total_segments = (size + MAX_SEGMENT_SIZE - 1) / MAX_SEGMENT_SIZE;
    
    // 将日志内容分段发送
    //LOG_INFO("预计分段数: %d", total_segments);

    size_t offset = 0;
    int segment_index = 0;

    while (offset < size) {
        // 为每个分段创建新的 JSON 对象
        cJSON *root = cJSON_CreateObject();
        cJSON *body = cJSON_CreateArray();

        // 添加时间戳
        char timestamp[32];
        time_t now = time(NULL);
        struct tm *tm_now = localtime(&now);
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_now);
        cJSON_AddStringToObject(root, "timestamp", timestamp);

        // 添加文件信息
        cJSON_AddStringToObject(root, "type", "log");
        cJSON_AddNumberToObject(root, "total_size", size);
        cJSON_AddNumberToObject(root, "segment_count", total_segments);

        size_t segment_size = size - offset;
        if (segment_size > MAX_SEGMENT_SIZE) {
            segment_size = MAX_SEGMENT_SIZE;
        }

        //LOG_INFO("处理分段 %d/%d - 大小: %zu bytes", 
        //         segment_index + 1, total_segments, segment_size);

        cJSON *item = cJSON_CreateObject();
        char* segment = (char*)malloc(segment_size + 1);
        if (!segment) {
            LOG_ERROR("分配内存失败");
            cJSON_Delete(item);
            cJSON_Delete(body);
            cJSON_Delete(root);
            return;
        }

        memcpy(segment, content + offset, segment_size);
        segment[segment_size] = '\0';

        cJSON_AddNumberToObject(item, "addr", 1);//上报日志的地址是1
        cJSON_AddNumberToObject(item, "segment", segment_index++);
        cJSON_AddStringToObject(item, "content", segment);
        cJSON_AddNumberToObject(item, "size", segment_size);
        cJSON_AddItemToArray(body, item);

        free(segment);
        offset += segment_size;
        
        // 每个片段单独发送
        cJSON_AddItemToObject(root, "body", body);
        char *json_str = cJSON_Print(root);
        if (!json_str) {
            LOG_ERROR("JSON序列化失败");
            cJSON_Delete(body);
            cJSON_Delete(root);
            return;
        }

        //LOG_DEBUG("发送分段 %d - JSON大小: %zu bytes", 
        //          segment_index, strlen(json_str));
        publish_message(json_str, topic);

        // 添加延时，避免消息发送太快
        usleep(100000);  // 100ms

        free(json_str);
        
        // 清理本次循环的 JSON 对象
        cJSON_Delete(root);
    }

    LOG_INFO("日志内容发送完成 - 总共发送 %d 个分段", segment_index);
}

// 上传日志文件
static void upload_log_file(const char* topic, const char* filename, 
                          const char* start_time, const char* end_time) {
    size_t size;
    char* content = read_log_file(filename, &size, start_time, end_time);
    if (content) {
        LOG_INFO("开始上传日志文件: %s (大小: %zu bytes, 时间范围: %s 到 %s)", 
                 filename, size, start_time, end_time);
        send_log_content(topic, content, size);
        free(content);
        LOG_INFO("日志文件上传完成: %s", filename);
    } else {
        LOG_INFO("在指定时间范围内未找到日志记录: %s", filename);
    }
} 