#ifndef  __MDHIM_DEBUG_H
#define __MDHIM_DEBUG_H

/*
  Define debugs
*/

/* TODO This will be a easier place to add mlog support */

//#define DELETE_DEBUG
//#define FIND_DEBUG
//#define FLUSH_DEBUG
//#define GET_DEBUG
//#define HASH_DEBUG
#define INIT_DEBUG
#define INSERT_DEBUG
#define MLINK_DEBUG
#define MDHIM_DEBUG
#define OPEN_DEBUG
#define QUEUE_DEBUG
#define TESTER_DEBUG
#define CREATE_DEBUG
#define CLOSE_DEBUG
#define READDATA_DEBUG
#define READAT_DEBUG
#define GETNUM_DEBUG
#define UNLINK_DEBUG

/*
  Define debug print statements
*/

#ifdef DELETE_DEBUG
#define PRINT_DELETE_DEBUG(format, args...) printf("MDHIM_DELETE_DEBUG: "format, ##args);
#else
#define PRINT_DELETE_DEBUG(format, args...)
#endif

#ifdef FIND_DEBUG
#define PRINT_FIND_DEBUG(format, args...) printf("MDHIM_FIND_DEBUG: "format, ##args);
#else
#define PRINT_FIND_DEBUG(format, args...)
#endif

#ifdef FLUSH_DEBUG
#define PRINT_FLUSH_DEBUG(format, args...) printf("MDHIM_FLUSH_DEBUG: "format, ##args);
#else
#define PRINT_FLUSH_DEBUG(format, args...)
#endif

#ifdef GET_DEBUG
#define PRINT_GET_DEBUG(format, args...) printf("MDHIM_GET_DEBUG: "format, ##args);
#else
#define PRINT_GET_DEBUG(format, args...)
#endif

#ifdef HASH_DEBUG
#define PRINT_HASH_DEBUG(format, args...) printf("MDHIM_HASH_DEBUG: "format, ##args);
#else
#define PRINT_HASH_DEBUG(format, args...)
#endif

#ifdef INIT_DEBUG
#define PRINT_INIT_DEBUG(format, args...) printf("MDHIM_INIT_DEBUG: "format, ##args);
#else
#define PRINT_INIT_DEBUG(format, args...)
#endif

#ifdef INSERT_DEBUG
#define PRINT_INSERT_DEBUG(format, args...) printf("MDHIM_INSERT_DEBUG: "format, ##args);
#else
#define PRINT_INSERT_DEBUG(format, args...)
#endif

#ifdef MDHIM_DEBUG
#define PRINT_MDHIM_DEBUG(format, args...) printf("MDHIM_DEBUG: "format, ##args);
#else
#define PRINT_MDHIM_DEBUG(format, args...)
#endif

#ifdef MLINK_DEBUG
#define PRINT_MLINK_DEBUG(format, args...) printf("MDHIM_LINK_DEBUG: "format, ##args);
#else
#define PRINT_MLINK_DEBUG(format, args...)
#endif

#ifdef OPEN_DEBUG
#define PRINT_OPEN_DEBUG(format, args...) printf("MDHIM_OPEN_DEBUG: "format, ##args);
#else
#define PRINT_OPEN_DEBUG(format, args...)
#endif

#ifdef CLOSE_DEBUG
#define PRINT_CLOSE_DEBUG(format, args...) printf("MDHIM_CLOSE_DEBUG: "format, ##args);
#else
#define PRINT_CLOSE_DEBUG(format, args...)
#endif

#ifdef CREATE_DEBUG
#define PRINT_CREATE_DEBUG(format, args...) printf("MDHIM_CREATE_DEBUG: "format, ##args);
#else
#define PRINT_CREATE_DEBUG(format, args...)
#endif

#ifdef READDATA_DEBUG
#define PRINT_READDATA_DEBUG(format, args...) printf("MDHIM_READDATA_DEBUG: "format, ##args);
#else
#define PRINT_READDATA_DEBUG(format, args...)
#endif

#ifdef READAT_DEBUG
#define PRINT_READAT_DEBUG(format, args...) printf("MDHIM_READAT_DEBUG: "format, ##args);
#else
#define PRINT_READAT_DEBUG(format, args...)
#endif

#ifdef UNLINK_DEBUG
#define PRINT_UNLINK_DEBUG(format, args...) printf("MDHIM_UNLINK_DEBUG: "format, ##args);
#else
#define PRINT_UNLINK_DEBUG(format, args...)
#endif

#ifdef GETNUM_DEBUG
#define PRINT_GETNUM_DEBUG(format, args...) printf("MDHIM_GETNUM_DEBUG: "format, ##args);
#else
#define PRINT_GETNUM_DEBUG(format, args...)
#endif

#ifdef QUEUE_DEBUG
#define PRINT_QUEUE_DEBUG(format, args...) printf("QUEUE_DEBUG: "format, ##args);
#else
#define PRINT_QUEUE_DEBUG(format, args...)
#endif

#ifdef TESTER_DEBUG
#define PRINT_TESTER_DEBUG(format, args...) printf("TEST_DEBUG: "format, ##args);
#else
#define PRINT_TESTER_DEBUG(format, args...)
#endif

#endif
