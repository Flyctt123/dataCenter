#include "mainwindow.h"
#include <QVBoxLayout>
#include <QMessageBox>

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent)
    , m_logReceiver(new LogReceiver(this))
{
    setupUi();

    // 连接信号槽
    connect(m_logReceiver, &LogReceiver::newLogReceived,
            this, &MainWindow::onNewLogReceived);
    connect(m_logReceiver, &LogReceiver::logCompleted,
            this, &MainWindow::onLogCompleted);
    connect(m_logReceiver, &LogReceiver::connectionStateChanged,
            this, &MainWindow::onConnectionStateChanged);
    connect(m_logReceiver, &LogReceiver::errorOccurred,
            this, &MainWindow::onErrorOccurred);

    // 连接到MQTT代理并订阅主题
    m_logReceiver->connectToBroker("192.168.10.100", 1883);
    m_logReceiver->subscribeToTopic("dataCenter/JSON/LD1/YX");
}

MainWindow::~MainWindow() {
    // 析构函数实现
}

void MainWindow::setupUi()
{
    QWidget* centralWidget = new QWidget(this);
    setCentralWidget(centralWidget);

    QVBoxLayout* layout = new QVBoxLayout(centralWidget);

    m_connectButton = new QPushButton("Connect", this);
    QPushButton* requestButton = new QPushButton("Request Logs", this);
    m_statusLabel = new QLabel("Disconnected", this);
    m_logView = new QPlainTextEdit(this);
    m_logView->setReadOnly(true);

    layout->addWidget(m_connectButton);
    layout->addWidget(requestButton);
    layout->addWidget(m_statusLabel);
    layout->addWidget(m_logView);

    resize(800, 600);

    // 连接请求按钮的点击事件
    connect(requestButton, &QPushButton::clicked, m_logReceiver, &LogReceiver::publishLogRequest);

    // 连接按钮点击事件
    connect(m_connectButton, &QPushButton::clicked, this, [this]() {
        if (m_logReceiver->state() == QMqttClient::Connected) {
            m_logReceiver->disconnectFromBroker();
        } else {
            m_logReceiver->connectToBroker("192.168.10.100", 1883);
        }
    });
}

void MainWindow::onNewLogReceived(const QString& content)
{
    m_logView->appendPlainText(content);
}

void MainWindow::onLogCompleted(const QString& filename)
{
    m_statusLabel->setText(tr("Log saved: %1").arg(filename));
}

void MainWindow::onConnectionStateChanged(bool connected)
{
    m_connectButton->setText(connected ? "Disconnect" : "Connect");
    m_statusLabel->setText(connected ? "Connected" : "Disconnected");
}

void MainWindow::onErrorOccurred(const QString& error)
{
    QMessageBox::warning(this, "Error", error);
} 