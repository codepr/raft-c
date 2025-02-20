#ifndef IOMUX_H
#define IOMUX_H

#include <sys/types.h>

typedef struct iomux iomux_t;
typedef enum iomux_event {
    IOMUX_READ  = 1 << 0, // 0x01
    IOMUX_WRITE = 1 << 1, // 0x02
} iomux_event_t;

iomux_t *iomux_create(void);
void iomux_free(iomux_t *mux);

int iomux_add(iomux_t *mux, int fd, iomux_event_t events);
int iomux_del(iomux_t *mux, int fd);
int iomux_wait(iomux_t *mux, time_t timeout_ms);

int iomux_get_event_fd(iomux_t *mux, int index);
iomux_event_t iomux_get_event_flags(iomux_t *mux, int index);

#endif
