#include "LogReceiver.h"
#include <QDir>
#include <QDebug>

// 添加数据存储结构
QHash<QString, QHash<int, double>> m_topicData;  // 主题 -> (地址 -> 值) 的映射

LogReceiver::LogReceiver(QObject *parent) 
    : QObject(parent)
    , m_client(new QMqttClient(this))
    , m_expectedSegments(0)
    , m_totalSize(0)
{
    m_savePath = "received_logs";
    QDir().mkpath(m_savePath);

    // 连接信号槽
    connect(m_client, &QMqttClient::connected, this, &LogReceiver::onConnected);
    connect(m_client, &QMqttClient::disconnected, this, &LogReceiver::onDisconnected);
    connect(m_client, &QMqttClient::errorChanged, this, &LogReceiver::onError);
}

LogReceiver::~LogReceiver()
{
    disconnectFromBroker();
}

/*SSH连接时触发*/
void LogReceiver::connectToBroker(QString host, quint16 port)
{
    // 设置客户端ID，避免重复连接
    QString clientId = QString("LogReceiver_%1").arg(QDateTime::currentMSecsSinceEpoch());
    m_client->setClientId(clientId);

    // 设置清理会话标志
    m_client->setCleanSession(true);

    // 设置保持连接时间
    m_client->setKeepAlive(60);

    // // 设置连接超时时间
    // m_client->setAutoKeepAlive(true);

    // // 设置自动重连
    // m_client->setAutoReconnect(true);
    // m_client->setAutoReconnectInterval(3000); // 3秒后自动重连

    qDebug() << "Connecting to MQTT broker at" << host << ":" << port;

    m_client->setHostname(host);
    m_client->setPort(port);

    // 设置连接参数（可选）
    // m_client->setUsername("your_username");
    // m_client->setPassword("your_password");

    m_client->connectToHost();
}

void LogReceiver::disconnectFromBroker()
{
    m_client->disconnectFromHost();
}

bool LogReceiver::subscribeToTopic(const QString& topic)
{
    if (m_client->state() == QMqttClient::Connected) {
        auto subscription = m_client->subscribe(QMqttTopicFilter(topic));
        if (!subscription) {
            emit errorOccurred(tr("Failed to subscribe to topic: %1").arg(topic));
            return true;
        }

        // 连接订阅的消息接收信号
        connect(subscription, &QMqttSubscription::messageReceived,
               this, [this](const QMqttMessage &msg) {
            onMessageReceived(msg.payload(), msg.topic());
        });

        qDebug() << "Successfully subscribed to topic:" << topic;
    }
    else
    {
        emit mqtt_disconnect_signal();
        return false;
    }
    return false;
}

/*发布日志查询命令*/
void LogReceiver::publishLogRequest()
{
    if (m_client->state() != QMqttClient::Connected) {
        emit errorOccurred("Not connected to MQTT broker");
        return;
    }

    // 直接构建 JSON 字符串，确保字段顺序
    QString timestamp = QDateTime::currentDateTime().toString("yyyy-MM-dd HH:mm:ss.zzz");
    QByteArray jsonData = QString(
        "{\n"
        "    \"token\": 0,\n"
        "    \"code\": 100,\n"
        "    \"priority\": 2,\n"
        "    \"cot\": 4,\n"
        "    \"timestamp\": \"%1\",\n"
        "    \"body\": [\n"
        "        {\n"
        "            \"addr\": 0,\n"
        "            \"val\": 0\n"
        "        }\n"
        "    ]\n"
        "}"
    ).arg(timestamp).toUtf8();

    // 发布消息
    auto result = m_client->publish(
        QMqttTopicName("dataCenter/JSON/LD1/YX"),
        jsonData
    );

    if (result == -1) {
        emit errorOccurred("Failed to publish message");
    } else {
        qDebug() << "Published log request message";
    }
}

/*发布历史数据查询命令*/
void LogReceiver::publishHistoryRequest(QString topic, int hours)
{
    if (m_client->state() != QMqttClient::Connected) {
        emit errorOccurred("Not connected to MQTT broker");
        return;
    }

    // 构建历史数据查询请求
    QString timestamp = QDateTime::currentDateTime().toString("yyyy-MM-dd HH:mm:ss.zzz");
    QByteArray jsonData = QString(
        "{\n"
        "    \"token\": 0,\n"
        "    \"code\": 100,\n"
        "    \"priority\": 2,\n"
        "    \"cot\": 4,\n"
        "    \"timestamp\": \"%1\",\n"
        "    \"body\": [\n"
        "        {\n"
        "            \"addr\": 2,\n"
        "            \"val\": %2,\n"
        "            \"topic\": \"%3\"\n"
        "        }\n"
        "    ]\n"
        "}"
    ).arg(timestamp).arg(hours).arg(topic).toUtf8();

    // 发布消息
    auto result = m_client->publish(
        QMqttTopicName("dataCenter/JSON/LD1/YX"),
        jsonData
    );

    if (result == -1) {
        emit errorOccurred("Failed to publish history request");
    } else {
        qDebug() << "Published history request for topic:" << topic << "hours:" << hours;
    }
}

/*发布事件查询命令*/
void LogReceiver::publishEventRequest(int hours)
{
    if (m_client->state() != QMqttClient::Connected) {
        emit errorOccurred("Not connected to MQTT broker");
        return;
    }

    // 构建历史数据查询请求
    QString timestamp = QDateTime::currentDateTime().toString("yyyy-MM-dd HH:mm:ss.zzz");
    QByteArray jsonData = QString(
        "{\n"
        "    \"token\": 0,\n"
        "    \"code\": 100,\n"
        "    \"priority\": 2,\n"
        "    \"cot\": 4,\n"
        "    \"timestamp\": \"%1\",\n"
        "    \"body\": [\n"
        "        {\n"
        "            \"addr\": 4,\n"
        "            \"val\": %2\n"
        "        }\n"
        "    ]\n"
        "}"
    ).arg(timestamp).arg(hours).toUtf8();

    // 发布消息
    auto result = m_client->publish(
        QMqttTopicName("dataCenter/JSON/LD1/YX"),
        jsonData
    );

    if (result == -1) {
        emit errorOccurred("Failed to publish event request");
    } else {
        qDebug() << "Published event request for" << hours << "hours";
    }
}

void LogReceiver::onConnected()
{
    qDebug() << "Connected to MQTT broker";
    emit connectionStateChanged(true);
    
    // 重新订阅主题
    subscribeToTopic("dataCenter/JSON/LD0/YC");
    subscribeToTopic("dataCenter/JSON/LD1/YX");
}

void LogReceiver::onDisconnected()
{
    emit connectionStateChanged(false);
    qDebug() << "Disconnected from MQTT broker";
}

void LogReceiver::onMessageReceived(const QByteArray& message, const QMqttTopicName& topic)
{
    qDebug() << "Received message on topic:" << topic.name();
    QString messageStr = QString::fromUtf8(message);
    QByteArray utf8Data = messageStr.toUtf8();
    QJsonDocument doc = QJsonDocument::fromJson(utf8Data);
    if (doc.isNull()) {
        emit errorOccurred("Invalid JSON message received");
        return;
    }

    QJsonObject root = doc.object();
    QString type = root["type"].toString();
    
    if (type == "log") {  // 处理日志消息
        processLogSegment(root);
    } else if (type == "history") {  // 处理历史数据
        processHistoryData(root);
    } else if (type == "event") {  // 处理事件记录
        processEventData(root);
    } else {  // 处理普通数据
        // 获取主题
        QString topicStr = topic.name();
  
        // 获取数据体
        QJsonArray body = root["body"].toArray();

        // 遍历数据体中的每个项目
        for (int i = 0; i < body.size(); ++i) {
            QJsonValue value = body.at(i);
            if (!value.isObject()) {
                qDebug() << "Invalid body item at index" << i;
                continue;
            }
  
            QJsonObject dataItem = value.toObject();
            if (dataItem.isEmpty()) {
                qDebug() << "Empty data item at index" << i;
                continue;
            }
  
            // 获取地址和值
            int addr = dataItem["addr"].toInt();
            double val = dataItem["val"].toDouble();
  
            // 存储数据
            m_topicData[topicStr][addr] = val;
        }
  
        // 打印存储的数据
        for (auto topicIt = m_topicData.constBegin(); topicIt != m_topicData.constEnd(); ++topicIt) {
            const QString& topic = topicIt.key();
            const QHash<int, double>& addrMap = topicIt.value();
            
            qDebug().noquote() << "主题:" << topic;
            for (auto addrIt = addrMap.constBegin(); addrIt != addrMap.constEnd(); ++addrIt) {
                qDebug().noquote() << QString("  地址: %1, 值: %2")
                                    .arg(addrIt.key())
                                    .arg(addrIt.value());
            }
        }
    }
}

void LogReceiver::processLogSegment(const QJsonObject& data)
{
    // 检查是否是新的日志文件
    QString timestamp = data["timestamp"].toString();
    if (timestamp != m_currentTimestamp) {
        clearCurrentLog();
        m_currentTimestamp = timestamp;
        m_totalSize = data["total_size"].toInt();
        m_expectedSegments = data["segment_count"].toInt();
    }

    // 处理日志片段
    QJsonArray body = data["body"].toArray();
    for (const auto& item : body) {
        QJsonObject segment = item.toObject();
        int segmentIndex = segment["segment"].toInt();
        // 确保内容以 UTF-8 编码处理
        QString content = QString::fromUtf8(segment["content"].toString().toUtf8());
        
        m_segments[segmentIndex] = content;
        emit newLogReceived(content);
    }

    // 检查是否接收完整
    if (m_segments.size() == m_expectedSegments) {
        saveCompleteLog(timestamp);
    }
}

void LogReceiver::processHistoryData(const QJsonObject& data)
{
    QString topic = data["topic"].toString();
    QString startTime = data["start_time"].toString();
    QString endTime = data["end_time"].toString();
    int count = data["count"].toInt();

    emit historyDataReceived(QString("历史数据查询结果 - 主题: %1\n时间范围: %2 到 %3\n记录数: %4\n")
                            .arg(topic).arg(startTime).arg(endTime).arg(count));

    QJsonArray body = data["body"].toArray();
    for (const auto& item : body) {
        QJsonObject obj = item.toObject();
        QString record = QString("地址: %1, 值: %2, 时间: %3, 描述: %4, 单位: %5\n")
                        .arg(obj["addr"].toInt())
                        .arg(obj["val"].toDouble())
                        .arg(obj["date"].toString())
                        .arg(obj["desc"].toString())
                        .arg(obj["unit"].toString());
        emit historyDataReceived(record);
    }
}

void LogReceiver::processEventData(const QJsonObject& data)
{
    QString startTime = data["start_time"].toString();
    QString endTime = data["end_time"].toString();
    int count = data["count"].toInt();

    emit eventDataReceived(QString("事件查询结果\n时间范围: %1 到 %2\n记录数: %3\n")
                          .arg(startTime).arg(endTime).arg(count));

    QJsonArray body = data["body"].toArray();
    for (const auto& item : body) {
        QJsonObject obj = item.toObject();
        QString record = QString("时间: %1\n描述: %2\n")
                        .arg(obj["date"].toString())
                        .arg(obj["desc"].toString());
        emit eventDataReceived(record);
    }
}

void LogReceiver::saveCompleteLog(QString& timestamp)
{
    QString filename = QString("%1/log_%2.txt")
                        .arg(m_savePath)
                        .arg(timestamp.replace(QRegExp("[: ]"), "_"));
    
    QFile file(filename);
    if (file.open(QIODevice::WriteOnly | QIODevice::Text)) {
        QTextStream out(&file);
        
        // 按顺序写入所有片段
        for (int i = 0; i < m_segments.size(); ++i) {
            out << m_segments[i];
        }
        
        file.close();
        emit logCompleted(filename);
        
        clearCurrentLog();
    } else {
        emit errorOccurred(tr("Failed to save log file: %1").arg(filename));
    }
}

void LogReceiver::clearCurrentLog()
{
    m_segments.clear();
    m_expectedSegments = 0;
    m_totalSize = 0;
    m_currentTimestamp.clear();
}

void LogReceiver::onError(QMqttClient::ClientError error)
{
    QString errorStr;
    switch (error) {
        case QMqttClient::NoError:
            errorStr = "No error";
            break;
        case QMqttClient::InvalidProtocolVersion:
            errorStr = "Invalid protocol version";
            break;
        case QMqttClient::IdRejected:
            errorStr = "Client ID rejected";
            break;
        case QMqttClient::ServerUnavailable:
            errorStr = "Server unavailable";
            break;
        case QMqttClient::BadUsernameOrPassword:
            errorStr = "Bad username or password";
            break;
        case QMqttClient::NotAuthorized:
            errorStr = "Not authorized";
            break;
        case QMqttClient::TransportInvalid:
            errorStr = "Transport invalid";
            break;
        case QMqttClient::ProtocolViolation:
            errorStr = "Protocol violation";
            break;
        case QMqttClient::UnknownError:
            errorStr = "Unknown error";
            break;
        case QMqttClient::Mqtt5SpecificError:
            errorStr = "MQTT5 specific error";
            break;
        default:
            errorStr = QString("Error code: %1").arg(error);
    }
    
    QString fullError = tr("MQTT Connection Error: %1").arg(errorStr);
    emit errorOccurred(errorStr);
    qDebug() << "MQTT Error:" << fullError 
             << "Host:" << m_client->hostname() 
             << "Port:" << m_client->port()
             << "State:" << m_client->state();
} 