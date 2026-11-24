#pragma once

#include <cstddef>
#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <sys/epoll.h>
#include <unordered_map>


class TcpServer
{
public:
    TcpServer();
    ~TcpServer();

    int run();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

private:
    enum class NonBlockingResult
    {
        SUCCESS,
        GET_FLAGS_FAILED,
        SET_FLAGS_FAILED
    };

    // Store per-client connection state
    struct ClientState
    {
        std::string pendingSend;
        bool readEOF = false;
    };

    static constexpr const char *PORT = "54000";
    static constexpr int MAX_EVENTS = 10;
    static constexpr std::size_t RECV_BUFFER_SIZE = 4096;

    int setupServer();
    int createListeningSocket();
    bool createEpollInstance();
    NonBlockingResult setNonBlocking(int socketFD);
    bool registerListeningSocket();

    void runEventLoop();
    void handleEvent(const epoll_event& event);
    void handleAccept(const epoll_event& event);
    bool registerClient(int clientSocket, const sockaddr_in& client);
    void logClientAddress(const sockaddr_in& client);

    void handleClientEvent(int currentFD, std::uint32_t events);
    bool handleClientRead(int currentFD, ClientState& state);
    bool handleClientWrite(int currentFD, ClientState& state);
    bool updateClientEvents(int currentFD, const ClientState& state);
    void cleanupClient(int currentFD);
    void closeServerResources() noexcept;

    int listeningSocket_ = -1;
    int epollFD_ = -1;
    std::unordered_map<int, ClientState> clients_;
};
