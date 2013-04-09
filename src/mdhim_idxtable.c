
#include "mdhim_idxtable.h"

/*
 * In index table design, transaction IDs will be stored as value for each key.
 * for example, user has inserted key "mykey" at transaction ID 1, 3, 6,10, then
 * we will maintain this info in index table like:
 *	"mykey" -> "1 3 6 10"
 * */

/*
 * reduce extra spaces in string
 *
 * 1. Drop spaces at head/tail of string
 * 2. Keep only one space if there are multiple sequential spaces
 *    in the middle of string
 *
 * \param str [IN]	target string
 * */
int _reduce_space(char *str)
{
	bool space = false;
	int i, loc = 0;
	for(i=0; str[i] != '\0'; ++i){
		if(str[i] != ' '){
			str[loc++] = str[i];
			space = true;
		} else if(space){
			str[loc++] = str[i];
			space = false;
		}
	}
	if(loc > 0 && str[loc-1] == ' '){
		str[loc-1] = '\0';
	} else {
		str[loc] = '\0';
	}

	return 0;
}

/*
 * returns valid token number from target string.
 * Here assumes target string contains at one valid token.
 *
 * For example, get_token_number("1 3 6 10", ' ') will tell us there are four
 * transaction IDs in list.
 *
 * \param str [IN]	target string
 * \param delimiter [IN]
 *			character separates valid tokens
 * */
int get_token_number(char *str, char delimiter)
{
	char *s = str;
	int i = 1;
	assert(s != NULL);
	while(s!=NULL){
		s = strchr(s,delimiter);
		if(s == NULL) break;
		s++; /* pass delimiter */
		/* jump over multiple delimiters */
		while(s!= NULL && *s == delimiter){
			s ++;
		}
		/* meeting end of string */
		if(*s == '\0') break;
		i ++;
	}
	return i;
}

/*
 * returns a pointer to the Nth occurrence of character T in target string.
 *
 * For example, find_token_location("1 3 6 10", ' ', 3) will return pointer
 * pointing to the space before 10, so we can easily read out transaction ID 10.
 *
 * \param str [IN]	target string
 * \param T [IN]	target character
 * \param Nth [IN]	the nth T
 *
 * */
char *find_token_location(char *str, char T, int Nth)
{
	char *s = str;
	int i = 0;

	while(s != NULL){
		if(i == Nth){
			return s;
		}
		s = strchr(s, T);
		if(s == NULL){
			return NULL;
		}
		i ++;
		s ++;
	}
	return NULL;
}

/*
 * insert transaction ID into TID list
 * Note: array points to a memory with size INDEXTABLEVALUESIZE
 *	 array must have already contains one TID at least
 *	 tid is not a magic one
 *
 * set next_tid if insert tid to head of buffer, then call will know
 * to update the tid cache of range [tid, next_tid).
 *
 * \param array [IN]	pointer to TID list
 * \param tid [IN]	transaction ID to be inserted
 * \param next_tid [IN/OUT]
 *			next transaction ID if inserting tid to buffer header
 * \param unlink [IN]	this is an unlink operation if set to true
 *
 * \return
 *	0 means insert successfully, caller need to update
 *	1 means insert successfully and next_tid is set
 *	-1 means array is left unchanged, caller need not to update
 * */
int insert_Tid(char *array, mdhim_trans_id_t tid, mdhim_trans_id_t *next_tid,
	       bool unlink)
{
	int ret = 0, numTids, i;
	mdhim_trans_id_t tmp;
	char *p = array;
	char list[INDEXTABLEVALUESIZE] = {'\0'};
	bool found = false; /* find larger one */

	if(array == NULL){
		return -1;
	}

	/* each TID is separated by space */
	numTids = get_token_number(array, ' ');

	/* find the first larger TID */
	for(i=0; i<numTids; i++){
		tmp = atol(p);
		/*XXX what if tmp is a magic one of tid ? */
		if( (tmp & UNLINKMAGICBIT) != 0 ){
			/* a magic one */
			if(REALTID(tmp) > tid){
				found = true;
				if(unlink){
					*next_tid = REALTID(tmp);
					ret = 1;
				}
				break;
			}else if(REALTID(tmp) == tid){
				if(unlink){
					/* do nothing */
					return -1;
				}
				/* XXX App unlink the key and then re-set the
				 * same key in same transaction ID.
				 * modify in position */
				memcpy(list, array, p - array);
				p = find_token_location(array, ' ', i+1);
				if(p != NULL){
					sprintf(list + strlen(list), " %lld %s",
							tid, p);
				} else {
					sprintf(list + strlen(list), " %lld",
							tid);
				}
				return 0;

			}
			/* a smaller one, continue */
			p = find_token_location(array, ' ', i+1);
			if(p == NULL) break;
			continue;
		}
		/* a normal tid */
		if(tmp == tid){
			if(unlink){
				/* user set it and then unlink it at same
				 * transaction ID, modify in position.
				 * */
				memcpy(list, array, p - array);
				p = find_token_location(array, ' ', i+1);
				if(unlink){
					tid = MAGICTID(tid);
					if(p != NULL) *next_tid = atol(p);
					ret = 1;
				}
				if(p != NULL){
					sprintf(list + strlen(list), " %lld %s",
							tid, p);
				} else {
					sprintf(list + strlen(list), " %lld",
							tid);
				}
				return ret;
			}
			/* overwriting an key, no need to update */
			return -1;
		} else if(tmp > tid) {
			/* find the first larger tid */
			found = true;
			if(i == 0 || unlink){
				/* inserting to head of idx buffer or it's
				 * unlink op */
				*next_tid = tmp;
				ret = 1;
			}
			break;
		}
		/* smaller tid, continue searching */

		p = find_token_location(array, ' ', i+1);
		if(p == NULL) break;
	}

	if(found == false){
		/* End of string, appending tid to tail */
		if(unlink){
			sprintf(list, "%s %lld", array, MAGICTID(tid) );
		} else {
			sprintf(list, "%s %lld", array, tid);
		}
	} else {
		/* p points to a space and indicates the first larger one */
		memcpy(list, array, p - array);
		if(unlink){
			tid = MAGICTID(tid);
		}
		sprintf(list + strlen(list), " %lld", tid);
		/* XXX do we need to check if there is unlink later?
		 * I don't think so. The only reason for this is there
		 * are multiple under-going transactions, a lower one
		 * insert a key and a higher one unlink it. They will
		 * both commit, so eventually, it will be consistency.
		 * */
		memcpy(list+strlen(list), p, array+strlen(array) - p);
	}

	if( strlen(list) > INDEXTABLEVALUESIZE){
		/* Error handling */
		return -1;
	}

	/* reduce extra space */
	_reduce_space(list);
	/* copy back */
	memcpy(array, list, strlen(list));
	return ret;
}

/*
 * Find most recent tid in TID list
 *
 * \param array [IN]	TID list that each TID sorted in ascending order
 * \param target [IN]	target transaction ID
 * \param retTID[IN/OUT]
 *			the maximum transaction ID less than target
 *
 *\return		zero on success, negative value if error
 *
 * */
int find_most_recent_tid(char *array, mdhim_trans_id_t target,
			 mdhim_trans_id_t *retTid)
{
	mdhim_trans_id_t tid = 0, tmp;
	int numTids = 0, i, found = 0, ret = 0;
	char *p = array;

	if(array == NULL) return -1;

	/* XXX searching from reverse order is more efficient?
	 * We will only cache limited tids, so it doesn't matter
	 * TODO this is a sorted list, so binary search will be better
	 * */
	numTids = get_token_number(array, ' ');
	for(i=0; i<numTids; i++){
		tmp = atol(p);
		if( (tmp & UNLINKMAGICBIT) != 0 ){
			/* a magic one */
			if(REALTID(tmp) >= tid){
				break;
			}
			continue;
		}
		if(tmp <= target){
			tid = tmp;
			found = 1;
		} else {
			break;
		}
		p = find_token_location(array, ' ', i+1);
		if(p == NULL) break;
	}

	if(found == 1) {
		*retTid = tid;
		ret = 0;
	} else {
		ret = -1;
	}
	return ret;
}

/*
 * update index table for key at transaction ID tid
 *
 * current status in index table: "key" --> "t1 t2 t10"
 * insert "key"@TID="t5" and we found first higher tid is "t10"
 * we need to maintain TID cache in following behavior,
 * "t1-t4"	: stay unchanged.
 * "t5-t9"	: add one more key ref count
 * "t10 - tN"	: stay unchanged.
 *
 * if not lowest tid, just an overwrite at different tid, don't care
 * if lowest tid, [tid - the next tid) add ref count
 *
 * \param fd [IN]	MDHIM fd
 * \param key [IN]	primary key in index table
 * \param tid [IN]	transaction ID
 * \param next_tid [IN/OUT]
 *			the frist higher transaction ID if there is one
 *
 * \return		zero on success and it's a new key,
 *			one on success and it's not a new key,
 *			negative value if error
 * */
int update_index_table(MDHIMFD_t *fd, char *key, mdhim_trans_id_t tid,
		       mdhim_trans_id_t *next_tid)
{
	int userKeyLen, outKeyLen, retValLen, ret;
	size_t idxBufSize;;
	char userKey[KEYSIZE] = {'\0'}, outUserKey[KEYSIZE] = {'\0'};
	char idxBuf[INDEXTABLEVALUESIZE] = {'\0'};

	// MDHIM must know details of user key and mdhim key because
	// it has to maintain index table
	userKeyLen = (find_token_location(key, ' ', 1) - key) -1;
	PRINT_MDHIM_DEBUG("%s: userKeyLen %d\n", __FUNCTION__, userKeyLen);
	memcpy(userKey, key, userKeyLen);
	ret = isamFindKey(fd->index_isamfd, MDHIM_EQ, 0, userKey,
			userKeyLen, outUserKey, &outKeyLen, &retValLen);
	PRINT_MDHIM_DEBUG("%s: isamFindKey %s ret %d\n", __FUNCTION__,
			userKey, ret);
	if (ret != MDHIM_SUCCESS){
		/* Not found user key in index table, insert it */
		sprintf(idxBuf, "%lld ", tid);
		ret = isamInsert(fd->index_isamfd, 1, &userKeyLen, userKey,
			strlen(idxBuf), idxBuf, &retValLen);
		PRINT_MDHIM_DEBUG("%s: insert %s to index table. ret %d\n",
				__FUNCTION__, userKey, ret);
	} else {
		/* read its value out, and update */
		ret = isamReadData(fd->index_isamfd, INDEXTABLEVALUESIZE,
				idxBuf, &idxBufSize);
		if(idxBufSize > INDEXTABLEVALUESIZE){
			fprintf(stderr, "%d exceeding limitation of index table"
			"entry size", __LINE__);
		}
		PRINT_MDHIM_DEBUG("%s: idxbuf %s\n", __FUNCTION__, idxBuf);
		ret = insert_Tid(idxBuf, tid, next_tid, false);
		PRINT_MDHIM_DEBUG("%s: after idxbuf %s\n", __FUNCTION__, idxBuf);
		if(ret >= 0){
			ret = isamUpdateData(fd->index_isamfd, strlen(idxBuf),
					idxBuf, (size_t *)&retValLen);
		}
		ret = 1;
	}

	return ret;
}

/*
 * insert transaction ID tid into object's TID cache
 *
 * next_tid will set to a non-zero value if tid is the first transaction
 * ID for certain key.
 * Only insert and unlink_key operations will affect TID cache
 *
 * \param fd [IN]	object's MDHIM fd
 * \param tid [IN]	transaction ID
 * \param ops [IN]	number of operations in this tid
 * \param newKey [IN]   true if it's a new key
 * \param next_tid [IN]
 *			0 or next transaction ID
 *
 * \return		zero on success, negative value if error
 * */
int insert_Tid_Cache(MDHIMFD_t *fd, mdhim_trans_id_t tid, int ops,
		     bool newKey, mdhim_trans_id_t next_tid)
{
	int ret = 0, i, numTids;
	bool found_tid = false;
	size_t tid_off = MAX_TID_SIZE;
	MDHIM_TID_INFO_t *tid_info = NULL, *tid_info_itr;
	MDHIM_TID_INFO_t *best_fit = NULL, *existing_itr = NULL;

	if( (tid_info = malloc(sizeof(MDHIM_TID_INFO_t))) == NULL){
		PRINT_MDHIM_DEBUG("%s: Error - OOM\n", __FUNCTION__);
		ret = MDHIM_ERROR_MEMORY;
	}

	numTids = pblListSize(fd->tidCache);
	tid_info->tid = tid;
	tid_info->count = ops;
	if(numTids > 0){
		for(i=0; i<numTids; i++){
			tid_info_itr = (MDHIM_TID_INFO_t *)pblListGet(fd->tidCache, i);
			if(tid > tid_info_itr->tid){
				if(tid - tid_info_itr->tid < tid_off){
					best_fit = tid_info_itr;
					tid_off = tid - tid_info_itr->tid;
				}
			} else if(tid == tid_info_itr->tid){
				existing_itr = tid_info_itr;
				found_tid = true;
			} else {
				assert(tid < tid_info_itr->tid);
				if(newKey || tid_info_itr->tid < next_tid){
					tid_info_itr->count += tid_info->count;
				}
			}
		}
		if(!found_tid){
			/* first time to insert this tid */
			if(newKey){
				if(best_fit != NULL){
					tid_info->count += best_fit->count;
				}
			} else {
				tid_info->count = best_fit->count;
			}
			pblListAdd(fd->tidCache, tid_info);
		} else {
			/* this tid has been inserted before */
			if(newKey){
				existing_itr->count += tid_info->count;
			}
			/* do nothing for old key */
			free(tid_info);
		}
	} else {
		assert(numTids == 0);
		/* an empty list */
		pblListAdd(fd->tidCache, tid_info);
	}

	/* only for debugging */
	dumpTidCache(fd);

	return ret;
}

/*
 * unlink transaction ID tid from object's TID cache
 *
 * de-reference count from transaction ID tid to next_tid
 *
 * \param fd [IN]	object's MDHIM fd
 * \param tid [IN]	transaction ID
 * \param next_tid [IN]
 *			next transaction ID
 *
 * \return		zero on success, negative value if error
 * */
int unlink_Tid_Cache(MDHIMFD_t *fd, mdhim_trans_id_t tid,
			mdhim_trans_id_t next_tid)
{
	int i, numTids, ret = 0;
	MDHIM_TID_INFO_t *tid_info_itr = NULL;

	numTids = pblListSize(fd->tidCache);
	if(numTids > 0){
		for(i=0; i<numTids; i++){
			tid_info_itr = (MDHIM_TID_INFO_t *)pblListGet(fd->tidCache, i);
			if( tid_info_itr->tid >= tid || tid_info_itr->tid < next_tid){
				tid_info_itr->count --;
			}
		}
	} else {
		assert(numTids == 0);
		/* an empty list */
		ret = -1;
	}
	return ret;
}

/*
 * get number of keys at transaction ID(tid) from object's TID cache
 *
 * \param fd [IN]	object's MDHIM fd
 * \param tid [IN]	transaction ID
 *
 * \return		zero on success, negative value if error
 * */
int get_numKeys_Tid_Cache(MDHIMFD_t *fd, mdhim_trans_id_t tid)
{
	int i, numTids, numKeys = 0;
	size_t tid_off = MAX_TID_SIZE;
	MDHIM_TID_INFO_t *tid_info_itr = NULL;

	numTids = pblListSize(fd->tidCache);
	if(numTids > 0){
		for(i=0; i<numTids; i++){
			tid_info_itr = (MDHIM_TID_INFO_t *)pblListGet(fd->tidCache, i);
			if(tid_info_itr->tid == tid){
				/* find it directly */
				numKeys = tid_info_itr->count;
				PRINT_MDHIM_DEBUG("%s: find tid %lu directly\n",
						__FUNCTION__, tid);
				break;
			} else if(tid_info_itr->tid < tid){
				/* find the nearest tid status */
				if ( tid - tid_info_itr->tid < tid_off ){
					tid_off = tid - tid_info_itr->tid;
					numKeys = tid_info_itr->count;
				}
			}
		}
	} else {
		/* an empty DB */
		numKeys = 0;
	}
	PRINT_MDHIM_DEBUG("%s: find %d keys in TID %lu\n", __FUNCTION__, numKeys, tid);
	return numKeys;
}

/*
 * free object's TID cache
 *
 * \param fd [IN]	object's MDHIM fd
 *
 * \return		zero on success, negative value if error
 * */
int freeTidCache(MDHIMFD_t *fd)
{
	int i, numTids;
	MDHIM_TID_INFO_t *tid_info_itr = NULL;

	numTids = pblListSize(fd->tidCache);
	for(i=0; i<numTids; i++){
		tid_info_itr = (MDHIM_TID_INFO_t *)pblListGet(fd->tidCache, i);
		free(tid_info_itr);
	}
	pblListFree(fd->tidCache);
	return 0;
}

/*
 * dump object's TID cache
 *
 * \param fd [IN]	object's MDHIM fd
 *
 * \return		zero on success, negative value if error
 * */
int dumpTidCache(MDHIMFD_t *fd)
{
	PRINT_MDHIM_DEBUG("Entring %s\n", __FUNCTION__);
	int i, numTids;
	MDHIM_TID_INFO_t *tid_info_itr = NULL;

	numTids = pblListSize(fd->tidCache);
	for(i=0; i<numTids; i++){
		tid_info_itr = (MDHIM_TID_INFO_t *)pblListGet(fd->tidCache, i);
		PRINT_MDHIM_DEBUG("%s: there are %d keys for TID %lld\n",
				__FUNCTION__, tid_info_itr->count, tid_info_itr->tid);
	}
	PRINT_MDHIM_DEBUG("Exiting %s\n", __FUNCTION__);
	return 0;
}
