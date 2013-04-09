#ifndef __MDHIM_MAP_H_
#define __MDHIM_MAP_H_

#include <stdio.h>
#include <stdbool.h>

#include "mdhim.h"

void *createMdhimMap();
int freeMdhimMap( void *map);

int setMdhimFd(char *str, MDHIMFD_t *fd);
void *delMdhimFd(char *str);
MDHIMFD_t *getMdhimFd(char *str);

#endif
