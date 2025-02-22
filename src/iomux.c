#include "iomux.h"

#include <stdlib.h>
#include <unistd.h>

#if defined(__APPLE__)

#include <sys/event.h>

#define NUM_EVENTS 512

struct iomux {
    int kq;
    struct kevent events[NUM_EVENTS];
    int nevents;
};

iomux_t *iomux_create(void)
{
    iomux_t *mux = malloc(sizeof(iomux_t));
    if (!mux)
        return NULL;
    mux->kq      = kqueue();
    mux->nevents = 0;
    return mux->kq >= 0 ? mux : (free(mux), NULL);
}

void iomux_free(iomux_t *mux)
{
    close(mux->kq);
    free(mux);
}

int iomux_add(iomux_t *mux, int fd, iomux_event_t events)
{
    short filter = 0;
    if (events & IOMUX_READ)
        filter |= EVFILT_READ;
    if (events & IOMUX_WRITE)
        filter |= EVFILT_WRITE;
    struct kevent ev;
    EV_SET(&ev, fd, filter, EV_ADD, 0, 0, NULL);
    return kevent(mux->kq, &ev, 1, NULL, 0, NULL);
}

int iomux_del(iomux_t *mux, int fd)
{
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    return kevent(mux->kq, &ev, 1, NULL, 0, NULL);
}

int iomux_wait(iomux_t *mux, time_t timeout_ms)
{
    struct timespec ts = {timeout_ms / 1000, (timeout_ms % 1000) * 1000000};
    mux->nevents       = kevent(mux->kq, NULL, 0, mux->events, NUM_EVENTS,
                          timeout_ms >= 0 ? &ts : NULL);
    return mux->nevents;
}

int iomux_get_event_fd(iomux_t *mux, int index)
{
    return mux->events[index].ident;
}

iomux_event_t iomux_get_event_flags(iomux_t *mux, int index)
{
    iomux_event_t mask = 0;
    short filter       = mux->events[index].filter;
    if (filter == EVFILT_READ)
        mask |= IOMUX_READ;
    if (filter == EVFILT_WRITE)
        mask |= IOMUX_WRITE;
    return mask;
}

#else

#include <string.h>
#include <sys/select.h>

#define NUM_EVENTS 1024

struct iomux {
    fd_set readfds;
    fd_set writefds;
    int maxfd;
    int fds[NUM_EVENTS];
    int nfds;
};

iomux_t *iomux_create(void)
{
    iomux_t *mux = malloc(sizeof(iomux_t));
    if (!mux)
        return NULL;
    FD_ZERO(&mux->readfds);
    FD_ZERO(&mux->writefds);
    mux->maxfd = -1;
    mux->nfds  = 0;
    for (int i = 0; i < NUM_EVENTS; ++i)
        mux->fds[i] = -1;
    return mux;
}

void iomux_free(iomux_t *mux) { free(mux); }

int iomux_add(iomux_t *mux, int fd, iomux_event_t events)
{
    if (mux->nfds >= NUM_EVENTS)
        return -1;
    if (events & IOMUX_READ)
        FD_SET(fd, &mux->readfds);
    if (events & IOMUX_WRITE)
        FD_SET(fd, &mux->writefds);
    mux->fds[mux->nfds++] = fd;
    if (fd > mux->maxfd)
        mux->maxfd = fd;
    return 0;
}

int iomux_del(iomux_t *mux, int fd)
{

    if (FD_ISSET(fd, &mux->readfds))
        FD_CLR(fd, &mux->readfds);
    if (FD_ISSET(fd, &mux->writefds))
        FD_CLR(fd, &mux->writefds);
    for (int i = 0; i < mux->nfds; i++) {
        if (mux->fds[i] == fd) {
            memmove(&mux->fds[i], &mux->fds[i + 1],
                    (mux->nfds - i - 1) * sizeof(int));
            mux->nfds--;
            break;
        }
    }
    return 0;
}

int iomux_wait(iomux_t *mux, time_t timeout_ms)
{
    FD_ZERO(&mux->readfds);
    for (int i = 0; i < mux->nfds; ++i) {
        if (mux->fds[i] == -1)
            continue;
        FD_SET(mux->fds[i], &mux->readfds);
    }
    struct timeval tv = {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    fd_set rfds       = mux->readfds;
    fd_set wfds       = mux->writefds;
    return select(mux->maxfd + 1, &rfds, &wfds, NULL,
                  timeout_ms >= 0 ? &tv : NULL);
}

int iomux_get_event_fd(iomux_t *mux, int index)
{
    return mux->fds[index]; // Simplified handling
}

iomux_event_t iomux_get_event_flags(iomux_t *mux, int index)
{
    int fd             = iomux_get_event_fd(mux, index);
    iomux_event_t mask = 0;
    if (FD_ISSET(fd, &mux->readfds))
        mask |= IOMUX_READ;
    if (FD_ISSET(fd, &mux->writefds))
        mask |= IOMUX_WRITE;
    return mask;
}

#endif
