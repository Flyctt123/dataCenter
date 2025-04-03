#include "logger.h"
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <locale.h>

// 日志系统配置
#define MAX_LOG_SIZE (10 * 1024 * 1024)  // 最大日志文件大小（10MB）
#define MAX_LOG_FILES 2                  // 最大保留的日志文件数量(dataCenter.log + dataCenter.log.1 + dataCenter.log.2)
#define MAX_PATH_LEN 256                  // 最大路径长度

// Logger implementation
static FILE* log_file = NULL;
static LogLevel current_level = LOGGER_INFO;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static char current_log_path[MAX_PATH_LEN] = {0};  // 保存当前日志文件路径

// 日志级别字符串
static const char* level_strings[] = {
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR"
};

// 检查并创建目录
static int ensure_directory(const char* path) {
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

// 获取当前时间字符串
static void get_time_str(char* buffer, size_t size) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    
    struct tm* tm_info = localtime(&tv.tv_sec);
    strftime(buffer, size, "%Y-%m-%d %H:%M:%S", tm_info);
    
    char msec[8];
    snprintf(msec, sizeof(msec), ".%03d", (int)(tv.tv_usec / 1000));
    strcat(buffer, msec);
}

// 检查文件大小
static long get_file_size(FILE* file) {
    long current_pos = ftell(file);
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fseek(file, current_pos, SEEK_SET);
    return size;
}

// 日志文件轮转
static void rotate_log_files(void) {
    char old_path[MAX_PATH_LEN], new_path[MAX_PATH_LEN];
    
    // 关闭当前日志文件
    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }
    
    // 删除最老的日志文件
    snprintf(old_path, sizeof(old_path), "%s.%d", current_log_path, MAX_LOG_FILES);
    remove(old_path);
    
    // 重命名现有的日志文件
    for (int i = MAX_LOG_FILES - 1; i >= 1; i--) {
        snprintf(old_path, sizeof(old_path), "%s.%d", current_log_path, i);
        snprintf(new_path, sizeof(new_path), "%s.%d", current_log_path, i + 1);
        rename(old_path, new_path);
    }
    
    // 重命名当前日志文件
    snprintf(new_path, sizeof(new_path), "%s.1", current_log_path);
    rename(current_log_path, new_path);
    
    // 重新打开日志文件
    log_file = fopen(current_log_path, "a");
    if (log_file) {
        char time_str[32];
        get_time_str(time_str, sizeof(time_str));
        fprintf(log_file, "\n[%s] [INFO] ========== 日志文件已轮转 ==========\n", time_str);
        fflush(log_file);
    }
}

// 检查并处理日志文件大小
static void check_log_size(void) {
    if (!log_file) return;
    
    long size = get_file_size(log_file);
    if (size >= MAX_LOG_SIZE) {
        rotate_log_files();
    }
}

int init_logger(const char* filename, LogLevel level) {
    // 设置本地化环境为 UTF-8
    setlocale(LC_ALL, "en_US.UTF-8");
    
    if (log_file) {
        fclose(log_file);
    }
    
    // 保存日志文件路径
    strncpy(current_log_path, filename, MAX_PATH_LEN - 1);
    
    // 确保日志目录存在
    ensure_directory(filename);
    
    log_file = fopen(filename, "a");
    if (!log_file) {
        perror("无法打开日志文件");
        return -1;
    }
    
    current_level = level;
    
    // 写入启动日志
    char time_str[32];
    get_time_str(time_str, sizeof(time_str));
    fprintf(log_file, "\n[%s] [INFO] ========== 日志系统启动 ==========\n", time_str);
    fflush(log_file);
    
    return 0;
}

void log_write(LogLevel level, const char* file, int line, const char* fmt, ...) {
    if (!log_file || level < current_level) {
        return;
    }
    
    pthread_mutex_lock(&log_mutex);
    
    // 检查日志文件大小
    check_log_size();
    
    char time_str[32];
    get_time_str(time_str, sizeof(time_str));
    
    // 获取文件名（不包含路径）
    const char* filename = strrchr(file, '/');
    filename = filename ? filename + 1 : file;
    
    // 写入日志头
    fprintf(log_file, "[%s] [%s] [%s:%d] ", 
            time_str, level_strings[level], filename, line);
    
    // 写入日志内容
    va_list args;
    va_start(args, fmt);
    vfprintf(log_file, fmt, args);
    va_end(args);
    
    fprintf(log_file, "\n");
    fflush(log_file);
    
    pthread_mutex_unlock(&log_mutex);
}

void close_logger(void) {
    if (log_file) {
        char time_str[32];
        get_time_str(time_str, sizeof(time_str));
        fprintf(log_file, "[%s] [INFO] ========== 日志系统关闭 ==========\n", time_str);
        fflush(log_file);
        fclose(log_file);
        log_file = NULL;
    }
} 