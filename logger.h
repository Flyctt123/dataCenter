#ifndef LOGGER_H
#define LOGGER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdarg.h>

// 日志级别定义
typedef enum {
    LOGGER_DEBUG = 0,
    LOGGER_INFO,
    LOGGER_WARN,
    LOGGER_ERROR
} LogLevel;

// 日志相关函数
int init_logger(const char* log_file, LogLevel level);
void log_write(LogLevel level, const char* file, int line, const char* fmt, ...);
void close_logger(void);

// 日志宏定义
#define LOG_DEBUG(fmt, ...) log_write(LOGGER_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  log_write(LOGGER_INFO,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  log_write(LOGGER_WARN,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) log_write(LOGGER_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif // LOGGER_H 