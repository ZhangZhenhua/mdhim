/*
Copyright (c) 2011, Los Alamos National Security, LLC. All rights reserved.
Copyright 2011. Los Alamos National Security, LLC. This software was produced under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National Laboratory (LANL), which is operated by Los Alamos National Security, LLC for the U.S. Department of Energy. The U.S. Government has rights to use, reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative works, such modified software should be clearly marked, so as not to confuse it with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
·         Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
·         Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
·         Neither the name of Los Alamos National Security, LLC, Los Alamos National Laboratory, LANL, the U.S. Government, nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/*
  A collection of functions to manipulate linked lists; insert, delete, print and search linked lists.

  Author: James Nunez and Medha Bhadkamkar

  Date: October 26, 2011

*/

#include "mdhim.h"

/* ========= whichStartRange ==========
   Find what range the input key belongs to

   Input:
   key is a pointer to the string to hash
   type is the type of key. Valid types are: 0 for char array, 1 for int and 2 for float
   maxRecordsPerRange is the size of the range

   Output:

   Return: starting range value or MDHIM_ERROR_KEY on error
*/
int whichStartRange(void *key, int type, int maxRecordsPerRange)
{

	int hashValue = 0, rangenum = 0;
	int startRange = -1;
	char strBuffer[256];

	/*
	   If the key is an int, float or double, the starting value of the
	   range is key (integer) divided by records per range.
	 */

	// XXX We could return range number if that would help, ie. if there
	// is code that calls which start range and looks for what range number it is.
	if (type == 0) {
		/*
		   If the key is a character string, the starting value of the
		   range is the hash value of the string
		 */
		strncpy(strBuffer, (char *)key, strlen(key));
		hashValue = (int)strBuffer[0];

		rangenum = (hashValue / maxRecordsPerRange);
		startRange = rangenum * maxRecordsPerRange;
		PRINT_HASH_DEBUG
		    ("whichStartRange: char type: key = %s, hash value = %d,"
		     "rangenum = %d, startRange = %d\n",
		     (char *)key, hashValue, rangenum, startRange);
	} else if (type == 1) {
		memset(strBuffer, '\0', 256);
		strncpy(strBuffer, (char *)key, strlen(key));
		PRINT_HASH_DEBUG
		    ("whichStartRange: Int type: key = %s, strlen(key) = %d and"
		     "strBuffer = %s\n",
		     (char *)key, (int)strlen(key), strBuffer);

		hashValue = atoi(strBuffer);
		startRange = hashValue / maxRecordsPerRange;
	} else if (type == 2) {
		memset(strBuffer, '\0', 256);
		strncpy(strBuffer, key, strlen(key));
		hashValue = atof(strBuffer);
		startRange = hashValue / maxRecordsPerRange;
	} else if (type == 3) {
		memset(strBuffer, '\0', 256);
		strncpy(strBuffer, key, strlen(key));
		hashValue = atof(strBuffer);
		startRange = hashValue / maxRecordsPerRange;
	} else {
		/*
		   Key type is not recognized
		 */
		printf
		    ("whichStartRange Error - the type of key %d is not"
		     "recognized\n", type);
		return MDHIM_ERROR_KEY;
	}

	return startRange;
}

/* ========= whichServer ==========
   Find what range server the key belongs.

   Return: the index of the server or MDHIM_ERROR_BASE on error
*/

/*
 * TODO
 * key starts with letters of alphabet, 'a' to 'z'. The repartioning
 * hash method used in semantic resharding uses a similar way.
 *
 * XXX now only supported for string type key.
 *
 * */
int whichServer(char *key, int max_records, int range_server_size)
{
	int index = 0, len, off, region, start, sec_idx;
	char first, second;

	len = strlen(key); /* key is NULL terminated */

	/*
	   Check for number of range servers. If there aren't any range servers,
	   return with an error.
	 */
	if (range_server_size < 1) {
		printf("whichServer Error: No range servers have been"
			"established; number of range servers is %d\n",
			range_server_size);
		return MDHIM_ERROR_BASE;
	}else if(range_server_size > MAX_RANGE_SRVS ){
		printf("%s Error: Exceeding maximum range server size\n",
				__FUNCTION__);
		return MDHIM_ERROR_BASE;
	}

	first = key[0];
	if(isupper(first)) first = tolower(first);

	/* shard the whole key space based on first char in string */
	if(len == 1 || range_server_size <= 26){ /*26 letters from 'a' to 'z'*/
		/* in such case, we can only hash to 26 range servers at most
		 * if there are 26 range servers, each will hold keys starting
		 * from 'a' to 'z' */

		/* the region size each range server will take care of,
		 * take 2 range servers for example,
		 * RS0              RS1
		 * 'a'-'m'          'n'-'z'
		 * */
		region = 26 / range_server_size;
		off = first - 'a';
		index = off / region;
		if(index >= range_server_size) index --;
	}else{
		assert(len > 1 && range_server_size > 26);
		second = key[1];
		if(isupper(second)) second = tolower(second);

		/* using the first two letters in key string, we can partion
		 * the key space into 26*26=676 range servers at most
		 * */

		/* how many range servers serve keys sharing the same first
		 * char in string
		 * take 52 range servers for example,
		 *
		 * RS0,RS1 RS2,RS3 ...... RS50,RS51
		 *    'a'    'b'             'z'
		 *
		 * */
		region = range_server_size / 26;
		assert(region <= 26);
		off = first - 'a';
		start = off * region;
		sec_idx = whichServer(key+1, max_records, region);
		index = start + sec_idx;
	}

	return index;
}

/* ========= getServerAndStartRange ==========
   Get the start range value the key belongs to and the range server number.
   Servers are round robin

   Input:
   fd is a pointer to the MDHIM data sttructure
   key is the starting range

   Output:

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/

int getServerAndStartRange(void *ikey, int key_type, int max_records,
			   int range_server_size, int *start_range, int *server)
{

	int err = MDHIM_SUCCESS;

	/*
	 */
	if ((*start_range = whichStartRange(ikey, key_type, max_records)) < 0) {
		printf
		    ("getServerAndStartRange Error: Can't find start range for"
		     "key %s.\n", (char *)ikey);
		return MDHIM_ERROR_BASE;
	}

	if ((*server =
	     whichServer(ikey, max_records, range_server_size)) < 0) {
		printf
		    ("getServerAndStartRange Error: Can't find server for key"
		     "%s.\n",(char *)ikey);
		return MDHIM_ERROR_BASE;
	}

	/*
	 */
	return err;
}
