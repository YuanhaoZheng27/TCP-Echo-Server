#include "TcpServer.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>


using namespace std;


TcpServer::TcpServer() = default;


TcpServer::~TcpServer()
{
    closeServerResources();
}


int TcpServer::run()
{
    int status = setupServer();

    if (status != 0)
    {
        closeServerResources();
        return status;
    }

    runEventLoop();

    closeServerResources();
    return 0;
}


int TcpServer::setupServer()
{
    int status = createListeningSocket();

    if (status != 0)
    {
        return status;
    }

    // Create epoll instance
    if (!createEpollInstance())
    {
        return -5;
    }

    // Set listening socket to non-blocking
    NonBlockingResult result = setNonBlocking(listeningSocket_);

    if (result == NonBlockingResult::GET_FLAGS_FAILED)
    {
        cerr << "Failed to get socket flags" << endl;
        return -6;
    }

    if (result == NonBlockingResult::SET_FLAGS_FAILED)
    {
        cerr << "Failed to set socket flags!" << endl;
        return -7;
    }

    if (!registerListeningSocket())
    {
        return -8;
    }

    return 0;
}


int TcpServer::createListeningSocket()
{
    // Create a socket
    int status;
    struct addrinfo hints;
    struct addrinfo *servinfo = nullptr;
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    status = getaddrinfo(
        nullptr,
        PORT,
        &hints,
        &servinfo
        );

    if (status != 0)
    {
        cerr << "getaddrinfo error: " << gai_strerror(status) << endl;
        return -1;
    }

    listeningSocket_ = socket(
        servinfo->ai_family,
        servinfo->ai_socktype,
        servinfo->ai_protocol
        );

    if (listeningSocket_ == -1)
    {
        cerr << "Can't create socket!" << endl;
        freeaddrinfo(servinfo);
        return -1;
    }

    // Bind the socket to a IP / port
    if (::bind(listeningSocket_,
        servinfo->ai_addr,
        servinfo->ai_addrlen
        ) == -1)
    {
        cerr << "Can't bind to IP/port" << endl;
        freeaddrinfo(servinfo);
        return -2;
    }

    freeaddrinfo(servinfo);

    // Mark the socket for listening in
    if (listen(listeningSocket_, SOMAXCONN) == -1)
    {
        cerr << "Can't listen!";
        return -3;
    }

    return 0;
}


bool TcpServer::createEpollInstance()
{
    epollFD_ = epoll_create1(0);

    if (epollFD_ == -1)
    {
        cerr << "Failed to create epoll instance!" << endl;
        return false;
    }

    return true;
}


TcpServer::NonBlockingResult TcpServer::setNonBlocking(int socketFD)
{
    int flags = fcntl(socketFD, F_GETFL, 0);

    if (flags == -1)
    {
        return NonBlockingResult::GET_FLAGS_FAILED;
    }

    if (fcntl(socketFD, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        return NonBlockingResult::SET_FLAGS_FAILED;
    }

    return NonBlockingResult::SUCCESS;
}


bool TcpServer::registerListeningSocket()
{
    struct epoll_event event{};

    event.events = EPOLLIN;
    event.data.fd = listeningSocket_;

    if (epoll_ctl(
            epollFD_,
            EPOLL_CTL_ADD,
            listeningSocket_,
            &event
        ) == -1)
    {
        cerr << "Failed to register listening socket with epoll!" << endl;
        return false;
    }

    return true;
}


void TcpServer::runEventLoop()
{
    // Create an array to store ready events
    epoll_event events[MAX_EVENTS];

    // Accept sequential connections
    while (true)
    {
        int eventCount = epoll_wait(
            epollFD_,
            events,
            MAX_EVENTS,
            -1
            );

        if (eventCount == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            cerr << "epoll_wait failed!" << endl;
            break;
        }

        for (int i = 0; i < eventCount; i++)
        {
            handleEvent(events[i]);
        }
    }
}


void TcpServer::handleEvent(const epoll_event& event)
{
    int currentFD = event.data.fd;

    if (currentFD == listeningSocket_)
    {
        handleAccept(event);
    }
    else
    {
        handleClientEvent(currentFD, event.events);
    }
}


void TcpServer::handleAccept(const epoll_event& event)
{
    if (!(event.events & EPOLLIN))
    {
        cerr << "Unexpected listening socket event" << endl;
        return;
    }

    while (true)
    {
        sockaddr_in client{};
        socklen_t clientSize = sizeof(client);

        int clientSocket = accept(
            listeningSocket_,
            (sockaddr*)&client,
            &clientSize
            );

        if (clientSocket == -1)
        {
            if (errno == EINTR )
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }

            cerr << "Accept failed: " << strerror(errno) << endl;
            break;
        }

        registerClient(clientSocket, client);
    }
}


bool TcpServer::registerClient(
    int clientSocket,
    const sockaddr_in& client
    )
{
    NonBlockingResult result = setNonBlocking(clientSocket);

    if (result != NonBlockingResult::SUCCESS)
    {
        cerr << "Failed to set client socket non-blocking" << endl;
        close(clientSocket);
        return false;
    }

    epoll_event clientEvent{};

    clientEvent.events = EPOLLIN | EPOLLRDHUP;
    clientEvent.data.fd = clientSocket;

    if (epoll_ctl(
            epollFD_,
            EPOLL_CTL_ADD,
            clientSocket,
            &clientEvent
        ) == -1)
    {
        cerr << "Failed to register client socket" << endl;
        close(clientSocket);
        return false;
    }

    clients_.emplace(clientSocket, ClientState{});
    cout << "Client registered: FD " << clientSocket << endl;

    logClientAddress(client);
    return true;
}


void TcpServer::logClientAddress(const sockaddr_in& client)
{
    char host[NI_MAXHOST];
    char svc[NI_MAXSERV];
    memset(host, 0, NI_MAXHOST);
    memset(svc, 0, NI_MAXSERV);

    int result = getnameinfo(
        (sockaddr*)&client,
        sizeof(client),
        host,
        NI_MAXHOST,
        svc,
        NI_MAXSERV,
        0
        );

    if (result != 0)
    {
        inet_ntop(AF_INET, &client.sin_addr, host, NI_MAXHOST);
        cout << host << " connected on " << ntohs(client.sin_port) << endl;
    }
    else
    {
        cout << host << " connected on " << svc << endl;
    }
}


void TcpServer::handleClientEvent(
    int currentFD,
    std::uint32_t events
    )
{
    auto it = clients_.find(currentFD);

    if (it == clients_.end())
    {
        return;
    }

    ClientState& state = it->second;
    bool shouldContinue = true;

    // Handle client socket readable events
    if (!state.readEOF &&
        (events & (EPOLLIN | EPOLLHUP | EPOLLRDHUP)))
    {
        shouldContinue = handleClientRead(currentFD, state);
    }

    // Handle socket errors
    if (events & EPOLLERR)
    {
        cerr << "Socket error: FD " << currentFD << endl;
        shouldContinue = false;
    }

    // Send echo data
    if (shouldContinue)
    {
        shouldContinue = handleClientWrite(currentFD, state);
    }

    // Close after sending all remaining data
    if (state.readEOF && state.pendingSend.empty())
    {
        shouldContinue = false;
    }

    // Update epoll events
    if (shouldContinue)
    {
        shouldContinue = updateClientEvents(currentFD, state);
    }

    // Close socket
    if (!shouldContinue)
    {
        cleanupClient(currentFD);
    }
}


bool TcpServer::handleClientRead(
    int currentFD,
    ClientState& state
    )
{
    char buf[RECV_BUFFER_SIZE];

    while (true)
    {
        ssize_t bytesRecv = recv(
            currentFD,
            buf,
            sizeof(buf),
            0
        );

        if (bytesRecv > 0)
        {
            cout << "Received: " << string(buf, bytesRecv) << endl;

            // Save received data for echo
            state.pendingSend.append(buf, bytesRecv);
        }
        else if (bytesRecv == 0)
        {
            cout << "Client disconnected: FD " << currentFD << endl;

            state.readEOF = true;
            return true;
        }
        else
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return true;
            }

            cerr << "recv failed: " << strerror(errno) << endl;
            return false;
        }
    }
}


bool TcpServer::handleClientWrite(
    int currentFD,
    ClientState& state
    )
{
    while (!state.pendingSend.empty())
    {
        ssize_t bytesSent = send(
            currentFD,
            state.pendingSend.data(),
            state.pendingSend.size(),
            MSG_NOSIGNAL
        );

        if (bytesSent > 0)
        {
            state.pendingSend.erase(0, bytesSent);
        }
        else if (bytesSent == 0)
        {
            cerr << "No data was sent" << endl;
            return false;
        }
        else
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return true;
            }

            cerr << "There was a sending issue" << endl;
            return false;
        }
    }

    return true;
}


bool TcpServer::updateClientEvents(
    int currentFD,
    const ClientState& state
    )
{
    epoll_event clientEvent{};
    clientEvent.data.fd = currentFD;

    if (!state.readEOF)
    {
        clientEvent.events = EPOLLIN | EPOLLRDHUP;
    }

    if (!state.pendingSend.empty())
    {
        clientEvent.events |= EPOLLOUT;
    }

    if (epoll_ctl(
            epollFD_,
            EPOLL_CTL_MOD,
            currentFD,
            &clientEvent
        ) == -1)
    {
        cerr << "Failed to update client events" << endl;
        return false;
    }

    return true;
}


void TcpServer::cleanupClient(int currentFD)
{
    if (epollFD_ != -1)
    {
        epoll_ctl(epollFD_, EPOLL_CTL_DEL, currentFD, nullptr);
    }

    close(currentFD);
    clients_.erase(currentFD);
}


void TcpServer::closeServerResources() noexcept
{
    for (const auto& entry : clients_)
    {
        int clientFD = entry.first;

        if (epollFD_ != -1)
        {
            epoll_ctl(epollFD_, EPOLL_CTL_DEL, clientFD, nullptr);
        }

        close(clientFD);
    }
    clients_.clear();

    // Close listening
    if (listeningSocket_ != -1)
    {
        close(listeningSocket_);
        listeningSocket_ = -1;
    }

    // Close epoll instance
    if (epollFD_ != -1)
    {
        close(epollFD_);
        epollFD_ = -1;
    }
}
