
#include "mdhim_map.h"

extern PblMap *FdPerRangeServer_map;

// called when opening an KV object
int setMdhimFd(char *str, MDHIMFD_t *fd)
{
	int ret;

	sprintf(fd->fd_addr_str, "%p", fd);
	PRINT_MDHIM_DEBUG("%s: object ID %s --> fd %p\n",__FUNCTION__, str, fd);
	ret = pblMapAddStrStr(FdPerRangeServer_map, str, fd->fd_addr_str);
	if(ret < 0){
		PRINT_MDHIM_DEBUG("%s failed\n", __FUNCTION__);
	}else if(ret == 0){
		PRINT_MDHIM_DEBUG("%s overwrinting key %s\n", __FUNCTION__,str);
	}

	return ret;
}

// called when closing an KV object
void *delMdhimFd(char *str)
{
	size_t unused; // length of value returned
	return pblMapRemoveStr(FdPerRangeServer_map, str, &unused);
}

MDHIMFD_t *getMdhimFd(char *str)
{
	size_t unused; // length of value returned
	MDHIMFD_t *fd = NULL;
	char *fd_addr_str = NULL;
	fd_addr_str = (char*)pblMapGetStr(FdPerRangeServer_map, str, &unused);
	if(fd_addr_str != NULL){
		sscanf(fd_addr_str, "%p", &fd);
	}
	PRINT_MDHIM_DEBUG("%s: object ID %s , fd %p\n", __FUNCTION__, str, fd);
	return fd;
}

void *createMdhimMap()
{
	return (void *)pblMapNewTreeMap();
}

int freeMdhimMap( void *map)
{
	pblMapFree( (PblMap *)map);
	return 0;
}
