#ifndef __MDHIM_IDXTABLE_H_
#define __MDHIM_IDXTABLE_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "mdhim.h"

/*
 * this is the header file for index table and tid cache management.
 *
 * maintaining index table for each key is quite straight forward, the
 * transaction ID lists will be stored as value for each key as a ISAM file.
 * And the transaction IDs will be sorted in ascending order.
 *
 * Tid cache now caches total keys at each transaction ID. Its management
 * is kind of complicated, user might set/reset keys at different transaction
 * IDs in different order. We must handle it carefully.
 * */

int get_token_number(char *str, char delimiter);
char *find_token_location(char *str, char T, int Nth);

/* index table related */
int insert_Tid(char *array, mdhim_trans_id_t tid, mdhim_trans_id_t *next_tid,
		bool unlink);
int find_most_recent_tid(char *array, mdhim_trans_id_t target,
			 mdhim_trans_id_t *retTid);
int update_index_table(MDHIMFD_t *fd, char *key, mdhim_trans_id_t tid,
		       mdhim_trans_id_t *next_tid);

/* tid cache related */
int insert_Tid_Cache(MDHIMFD_t *fd, mdhim_trans_id_t tid, int ops,
		     bool newKey, mdhim_trans_id_t next_tid);
int unlink_Tid_Cache(MDHIMFD_t *fd, mdhim_trans_id_t tid,
			mdhim_trans_id_t next_tid);
int get_numKeys_Tid_Cache(MDHIMFD_t *fd, mdhim_trans_id_t tid);
int freeTidCache(MDHIMFD_t *fd);
int dumpTidCache(MDHIMFD_t *fd);

#endif
