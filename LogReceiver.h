#ifndef LOGRECEIVER_H
#define LOGRECEIVER_H

#include <QObject>
#include <QMap>
#include <QString>
#include <QFile>
#include <QDateTime>
#include <QtMqtt/qmqttclient.h>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonArray>
#include <QDebug>
#include <QHash>
#include <QList>

class LogReceiver : public QObject
{
    Q_OBJECT
public:
    explicit LogReceiver(QObject *parent = nullptr);
    ~LogReceiver();

    void connectToBroker(QString host = "localhost", quint16 port = 1883);
    void disconnectFromBroker();
    bool subscribeToTopic(const QString& topic);
    void publishLogRequest();  // 添加日志请求函数声明
    void publishHistoryRequest(int hours);  // 添加历史数据请求函数
    void publishEventRequest(int hours);

signals:
    void newLogReceived(const QString& content);  // 收到新的日志片段
    void logCompleted(const QString& filename);   // 完整日志接收完成
    void connectionStateChanged(bool connected);
    void errorOccurred(const QString& error);
    void historyDataReceived(const QString& data);  // 添加历史数据信号
    void eventDataReceived(const QString& data);

private slots:
    void onConnected();
    void onDisconnected();
    void onMessageReceived(const QByteArray& message, const QMqttTopicName& topic);
    void onError(QMqttClient::ClientError error);

private:
    // 处理日志片段相关函数
    void processLogSegment(const QJsonObject& data);
    void processHistoryData(const QJsonObject& data);  // 添加历史数据处理函数
    void processEventData(const QJsonObject& data);
    void saveCompleteLog(QString& timestamp);
    void clearCurrentLog();

    // 辅助函数
    void createLogDirectory();
    bool isValidJsonMessage(const QJsonDocument& doc);
    bool isLogMessage(const QJsonObject& root);
    void handleLogMessage(const QJsonObject& root);
    void appendLogSegment(const QJsonObject& segment);
    bool shouldSaveLog() const;
    QString generateLogFilename(const QString& timestamp) const;
    bool writeLogToFile(const QString& filename);
    void notifyLogSaved(const QString& filename);

private:
    QMqttClient* m_client;
    QMap<int, QString> m_segments;      // 存储日志片段
    int m_expectedSegments;             // 预期的片段总数
    qint64 m_totalSize;                // 日志总大小
    QString m_currentTimestamp;         // 当前日志的时间戳
    QString m_savePath;                 // 日志保存路径
    bool m_isProcessingLog;             // 是否正在处理日志
    int m_lastSegmentIndex;             // 最后处理的片段索引
};

#endif // LOGRECEIVER_H 