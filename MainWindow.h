#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QLabel>
#include "LogReceiver.h"

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();

private slots:
    void onNewLogReceived(const QString& content);
    void onLogCompleted(const QString& filename);
    void onConnectionStateChanged(bool connected);
    void onErrorOccurred(const QString& error);

private:
    void setupUi();

private:
    LogReceiver* m_logReceiver;
    QPlainTextEdit* m_logView;
    QPushButton* m_connectButton;
    QLabel* m_statusLabel;
};

#endif // MAINWINDOW_H 