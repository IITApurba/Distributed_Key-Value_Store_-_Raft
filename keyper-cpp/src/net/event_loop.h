#pragma once
// Portable event-loop wrapper: epoll on Linux (the production target),
// kqueue on macOS/BSD (dev-machine builds), same three-call surface.
#include <vector>
#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace kvraft {

class EventLoop {
public:
    EventLoop() {
#if defined(__linux__)
        fd_ = epoll_create1(0);
#else
        fd_ = kqueue();
#endif
    }
    ~EventLoop() { if (fd_ >= 0) close(fd_); }

    void addRead(int watchFd) {
#if defined(__linux__)
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = watchFd;
        epoll_ctl(fd_, EPOLL_CTL_ADD, watchFd, &ev);
#else
        struct kevent ev;
        EV_SET(&ev, watchFd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
        kevent(fd_, &ev, 1, nullptr, 0, nullptr);
#endif
    }

    void remove(int watchFd) {
#if defined(__linux__)
        epoll_ctl(fd_, EPOLL_CTL_DEL, watchFd, nullptr);
#else
        struct kevent ev;
        EV_SET(&ev, watchFd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        kevent(fd_, &ev, 1, nullptr, 0, nullptr);
#endif
    }

    // Returns fds that became readable (or hung up); `hungUp` marks which of
    // the returned fds saw an error/EOF condition at the OS level.
    std::vector<std::pair<int, bool>> wait(int timeoutMs) {
        std::vector<std::pair<int, bool>> ready;
#if defined(__linux__)
        std::vector<epoll_event> events(256);
        int n = epoll_wait(fd_, events.data(), static_cast<int>(events.size()), timeoutMs);
        for (int i = 0; i < n; i++) {
            bool hup = events[i].events & (EPOLLHUP | EPOLLERR);
            ready.emplace_back(events[i].data.fd, hup);
        }
#else
        struct kevent events[256];
        struct timespec ts{timeoutMs / 1000, (timeoutMs % 1000) * 1000000};
        int n = kevent(fd_, nullptr, 0, events, 256, &ts);
        for (int i = 0; i < n; i++) {
            bool hup = events[i].flags & EV_EOF;
            ready.emplace_back(static_cast<int>(events[i].ident), hup);
        }
#endif
        return ready;
    }

private:
    int fd_ = -1;
};

} // namespace kvraft
