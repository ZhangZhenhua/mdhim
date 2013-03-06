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
   The Multi-Dimensional Hashed Indexed Middleware (MDHIM) System core routines
   
   Author: James Nunez and Medha Bhadkamkar
   
   Date: November 29, 2011
   
   Note:
   Error handling is primitive and needs to be improved
*/

#include "mdhim.h"     

/* ========== getCommands ==========
   
   Routine called by threads to act on range server commands 
   
   fd is the input MDHIM fd struct with information on range servers

   Valid commands are:
   flush - moves range data from all range servers to the first range server 
   in the range servers array

   Warning: The order of the pblIsamFile_t pointer array may not be the same 
   order of the final range_list because ranges are inserted into the 
   range_list and can move. The elements in the isam pointer array do not 
   track those moves. 

   Return: 
*/

void *getCommands(void *infd){
  char *command = NULL, *databuf = NULL, *keybuf = NULL;
  char **filenames, *dataname;
  char *p;
  char recv_buf[DATABUFFERSIZE];
  char recv_output[DATABUFFERSIZE];
  char *fileSettag = NULL;

  int indx, f_indx, r_indx, do_work;
  int rc, i, j, err = MDHIM_SUCCESS, dowork=1;      
  int server_rank = -1;
  int skip_len, len, totalLen;
  int out_int, recordNum;
  int parse_int1 = 0, parse_int2 = 0, *keyLen = NULL;
  int start_range, num_ranges = 0, max_ops = 0;
  int *intArray = NULL, *errArray = NULL;
  int num_dirty = 0, numFlushRanges = 0;
  unsigned long tempULong = 0, bitVector = 0;
  
  struct rangeDataTag *curRange, *tempRangeData;
  struct rangeDataTag *range_ptr, *cur_flush, *prev_flush;
  MDHIMFD_t *fd = (MDHIMFD_t* )infd;
  FILE *filed = NULL;
  int isamfd_counter = 0;
  pblIsamFile_t **isamfds;

  MPI_Status status;
  MPI_Request op_request, recv_request;
  
  while(dowork){
    memset(recv_buf, '\0', 2048);
    PRINT_MDHIM_DEBUG ("Rank %d: THREAD in get_mdhim_commands\n", fd->mdhim_rank);
    
    /*
      Post a non-blocking receive and wait/sleep until it returns. Also 
      wait/sleep for the operation Isend to complete.
    */
    if (MPI_Irecv(recv_buf, 2048, MPI_CHAR, MPI_ANY_SOURCE, SRVTAG, fd->mdhim_comm, &recv_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d: getCommands Error -  MPI_Irecv error.\n", fd->mdhim_rank);
    }

    receiveReady(&recv_request, &status);
    
    /*
      Look for recognized commands
    */
    
    if (!strncmp(recv_buf, "close", strlen("close"))){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      
      PRINT_MDHIM_DEBUG ("Rank %d: On close: bitVector is %lu\n", fd->mdhim_rank, bitVector);
      curRange = fd->range_data.range_list;
      for(j = 0; j < fd->range_data.num_ranges; j++){
	// XXX Make this an error array	
	err = MDHIM_SUCCESS;
	indx = (curRange->range_start/fd->max_recs_per_range)/fd->rangeSvr_size;
	curRange = curRange->next_range;
	tempULong = 1 << indx;
	//	tempULong = bitVector >> indx;
	
	if( tempULong & bitVector){
	  //	if( tempULong%2){
	  PRINT_MDHIM_DEBUG ("Rank %d: closing isam indx %d.\n", fd->mdhim_rank, indx);
	  
	  err = dbClose(isamfds[indx]);
	  /*
	    If we closed the file, zero out the bitVector at bit indx
	  */
	  if(!err){
	    tempULong = 1 << indx;
	    bitVector  = bitVector ^ tempULong;
	    
	    PRINT_MDHIM_DEBUG ("Rank %d: End of close, now bitvector = %lu\n", fd->mdhim_rank, bitVector);
	  }
	  
	  PRINT_MDHIM_DEBUG ("Rank %d: returned from dbClose with error = %d.\n", fd->mdhim_rank, err);
	}
      } /* end for(j = 0; j < fd->range_data.num_ranges; j++){ */
      //XXX send back an error array
      MPI_Isend(&err, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &op_request);
      
    }
    else if (!strncmp(recv_buf, "create", strlen("create"))){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
    }
    else if (!strncmp(recv_buf, "dbflush", strlen("dbflush"))){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
    }
    else if (!strncmp(recv_buf, "delete", strlen("delete"))){  
      PRINT_DELETE_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);

      /*
	Unpack the delete command message. The "delete" string is first, then 
	the start range, key index to search on and the key to delete.
      */
      if( (databuf = (char *)malloc(sizeof(recv_buf))) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the input key to delete.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      memset(databuf, '\0', sizeof(recv_buf));
      memset(recv_output, '\0', 2048);
      sscanf(recv_buf, "%*s %d %d %s", &start_range, &parse_int1, databuf);
      
      /* 
	 Delete the key from the database
      */
      indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;  
      err = dbDeleteKey(isamfds[indx], databuf, parse_int1, recv_output, &recordNum);
      
      PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - dbDeleteKey exited with deleted key %s with record number %d with err %d\n", fd->mdhim_rank, recv_output, recordNum, err);
      
      memset(recv_output, '\0', 2048);
      if(err != MDHIM_SUCCESS){
	/*
	  Key does not exist in the DB or there was another problem. 
	  Pass on the error value.
	*/
	recordNum = -1;
	out_int = -1;
      }
      else{
	/* 
	   The key was successfully deleted from the database. We need to 
	   modify the range data and set the current record. Flush data is 
	   not updated on delete.
	   
	   For all cases, set the current record to the one after the 
	   deleted key. If the deleted key is the last record, set the 
	   current record to the one before the deleted key. 
	*/
	rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
	
	//XXX I could point curRange to the node with deleted key using searchList, but not get index back.
	r_indx = -1;
	curRange = fd->range_data.range_list;
	for(i = 0; i < fd->range_data.num_ranges; i++){
	  if( curRange->range_start == start_range){
	    r_indx = i;
	    i = fd->range_data.num_ranges;
	  }
	  else{
	    curRange = curRange->next_range;
	  }
	}

	if((r_indx < 0) || (cur_flush == NULL)){
	  printf("Rank %d getCommands: Error - Cannot find flush or range (%d) information for the current deleted key with start range %d.\n", fd->mdhim_rank, r_indx, start_range);
	  err = MDHIM_ERROR_BASE;
	}
	
	PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - Range indx %d server has/had %d records and is dirty %d.\n", fd->mdhim_rank, r_indx, curRange->num_records, curRange->dirty_range);
	
	curRange->num_records--;
	curRange->dirty_range = 1;
	
	/*
	  If we deleted only key in range, don't delete the range. We will 
	  delete any empty ranges during flush.
	*/
	// XXX start modifying deleting only key here
	if(curRange->num_records == 0){
	  
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - deleted only key in range. Range %d has %d keys and server has %d ranges.\n", fd->mdhim_rank, r_indx, curRange->num_records, fd->range_data.num_ranges);
	  
	  // comment this out - don't want to delete
	  //	  searchAndDeleteNode(&(fd->range_data.range_list), start_range);
	  
	  // comment this out - don't want to change number of ranges
	  //	  fd->range_data.num_ranges--;

	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - after delete node, server has %d ranges.\n", fd->mdhim_rank, fd->range_data.num_ranges);
	  
	  // change this to fd->range_data.num_ranges == 1 (deleted only range)
	  //	  if(fd->range_data.num_ranges == 0){ 
	  if(fd->range_data.num_ranges == 1){ 
	    
	    /*
	      We deleted the only key from the only range.
	    */
	    
	    out_int = -1;
	    recordNum = -1;
	    recv_output[0] = '\0';
	  }
	  else if(cur_flush->next_range != NULL ){ 
	    
	    /*
	      We deleted a range other than the last range. The new current 
	      key is the minimum key of the next range.
	    */
	    
	    strcpy(recv_output, cur_flush->next_range->range_min);
	    recordNum = 1;
	    out_int = strlen(recv_output);
	  }
	  else{ 
	    
	    /*
	      We deleted the last range. The new current key is maximum 
	      of the previous range.
	    */
	    
	    strcpy(recv_output, prev_flush->range_max);
	    recordNum = prev_flush->num_records;
	  }
	}
	else if(!strcmp(databuf, curRange->range_min)){
	  /*
	    More than one record in range and we deleted the minimum record. 
	    Replace the minimum key in the range with the one found. 
	  */
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - More than one key and delete range min %s with deleted key %s with record number %d\n", fd->mdhim_rank, curRange->range_min, databuf, recordNum);

	  parse_int2 = recordNum;
	  err = dbGetKey(isamfds[indx], parse_int2, 0, parse_int1, recv_output, &out_int, &recordNum);
	  strcpy(curRange->range_min, recv_output);

	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - After dbgetKey new range min %s with new current key %s with record number %d\n", fd->mdhim_rank, curRange->range_min, recv_output, recordNum);
	}
	else if(!strcmp(databuf, curRange->range_max)){
	  /*
	    More than one record in the range and we deleted the maximum record.
	    Replace the maximum key for the range with the found key. 
	    If we deleted last key in the last range, set the current key to 
	    the found key else set it to the minimum of the next range. 
	  */
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - More than one key and delete range max %s with deleted key %s with record number %d\n", fd->mdhim_rank, curRange->range_max, databuf, recordNum);

	  parse_int2 = recordNum - 1;
	  err = dbGetKey(isamfds[indx], parse_int2, 0, parse_int1, recv_output, &out_int, &recordNum);
	  strcpy(curRange->range_max, recv_output);

	  if(cur_flush->next_range != NULL ){ 
	    /*
	      We deleted a range other than the last range. The new current 
	      key is the minimum key of the next range.
	    */
	    memset(recv_output, '\0', 2048);
	    strcpy(recv_output, cur_flush->next_range->range_min);
	    recordNum = 1;
	    out_int = strlen(recv_output);
	  }
	  
	}
	else{
	  /*
	    More than one record in the range and it was not the minimum nor 
	    maximum key. So, get the current key.
	  */
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - More than one key and middle key with deleted key %s with record number %d\n", fd->mdhim_rank, databuf, recordNum);
	  
	  memset(recv_output, '\0', 2048);
	  parse_int2 = recordNum;
	  err = dbGetKey(isamfds[indx], parse_int2, 0, parse_int1, recv_output, &out_int, &recordNum);
	  
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - Delete middle of the range with current key %s with record number %d.\n", fd->mdhim_rank, recv_output, recordNum);
	}
      }
      
      /*
	Now pack and send the error code to the requesting process. 
	The send can be non-blocking since this process doesn't 
	need to wait to see or act on if the send is received.
      */
      memset(databuf, '\0', sizeof(databuf));
      if(sprintf(databuf, "%d %d %d %s", err, recordNum, out_int, recv_output) < 0){
	printf("Rank %d getCommands: Error - problem packing output results for delete.\n", fd->mdhim_rank);
      }
      
      PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - going to send output %s with size %d to source with rank %d\n", fd->mdhim_rank, databuf, (int)strlen(databuf), status.MPI_SOURCE);
      
      MPI_Isend(databuf, strlen(databuf), MPI_BYTE, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
      free(databuf);      
    }
    else if (!strncmp(recv_buf, "find", strlen("find"))){  
      PRINT_FIND_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;
      
      /*
	Unpack the find command message. First is "find", then the start range, 
	key index to search on, type of find comparison and a search key.
      */
      if( (databuf = (char *)malloc( sizeof(recv_buf))) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the input key to find.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      memset(databuf, '\0', sizeof(recv_buf));
      
      sscanf(recv_buf, "%*s %d %d %d %s", &start_range, &parse_int1, &parse_int2, databuf);
      
      indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
      err = isamFindKey(isamfds[indx], parse_int2, parse_int1, databuf, (int)strlen(databuf), recv_output, &out_int, &recordNum);
      
      PRINT_FIND_DEBUG ("Rank %d: THREAD Find - going to send found key %s, return code %d, and found key len %d and record number %d.\n", fd->mdhim_rank, recv_output, err, out_int, recordNum);
      
      /*
	Now pack and send the error code and key found to the requesting 
	process. The send can be non-blocking since this process doesn't 
	need to wait to see or act on if the send is received.
      */
      memset(databuf, '\0', sizeof(recv_buf));
      if(sprintf(databuf, "%d %d %d %s", err, recordNum, out_int, recv_output) < 0){
	printf("Rank %d getCommands: Error - problem packing output results for find\n", fd->mdhim_rank);
      }
      
      PRINT_FIND_DEBUG ("Rank %d: THREAD Find - going to send output %s with size %d to source with rank %d\n", fd->mdhim_rank, databuf, (int)strlen(databuf), status.MPI_SOURCE);
      
      MPI_Isend(databuf, strlen(databuf), MPI_BYTE, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
      PRINT_FIND_DEBUG ("Rank %d: THREAD Find - Returned from sending done to %d. DONE with FIND\n", fd->mdhim_rank, status.MPI_SOURCE);

      free(databuf);      
    }
    else if(!strncmp(recv_buf, "flush", strlen("flush"))){
      err = MDHIM_SUCCESS;
      MPI_Comm_rank(fd->rangeSrv_comm, &server_rank);
      PRINT_FLUSH_DEBUG ("Rank %d: THREAD Inside %s with Range server comm size %d and server rank %d\n", fd->mdhim_rank, recv_buf, fd->rangeSvr_size, server_rank);
      
      /*
	Compute the number of dirty ranges each server has and then send it 
	to the first range server in the range_srv_info array. Then sum up 
	the number of ranges that will be sent you.
      */
      num_dirty = 0;
      range_ptr = fd->range_data.range_list;
      for(i = 0; i < fd->range_data.num_ranges; i++){
	if(range_ptr->dirty_range){
	  num_dirty++;
	}
	range_ptr = range_ptr->next_range;
      }
      
      PRINT_FLUSH_DEBUG ("Rank %d getCommands: Server rank %d has %d dirty ranges.\n", fd->mdhim_rank, server_rank, num_dirty);
      
      if(server_rank == 0){
	if( (intArray = (int *)malloc(sizeof(int) * fd->rangeSvr_size)) == NULL){
	  printf("Rank %d getCommands: Error - Unable to allocate memory for the array of range server data.\n", fd->mdhim_rank);
	  err = MDHIM_ERROR_MEMORY;
	}
      }
      
      PRINT_FLUSH_DEBUG ("Rank %d getCommands: Server rank %d before gather.\n", fd->mdhim_rank, server_rank);
      MPI_Gather(&num_dirty, 1, MPI_INT, intArray, 1, MPI_INT, 0, fd->rangeSrv_comm);
      
      if(server_rank == 0){
	numFlushRanges = 0;
	
	for(i = 0; i < fd->rangeSvr_size; i++){
	  numFlushRanges += intArray[i];
	  PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range server %d has %d dirty ranges\n", fd->mdhim_rank, i, intArray[i]);
	}
	
	/* 
	   If there are no dirty ranges, just send error codes to 
	   the requesting process.
	*/
	if(numFlushRanges == 0){
	  PRINT_FLUSH_DEBUG ("Rank %d: THREAD - There are no new ranges to flush. There are %d existing ranges in flush list.\n", fd->mdhim_rank, fd->flush_list.num_ranges);
	  
	  MPI_Isend(&err, 1, MPI_INT, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
	  continue;
	}
	
	PRINT_FLUSH_DEBUG ("Rank %d getCommands: Total number of dirty ranges %d\n", fd->mdhim_rank, numFlushRanges);
	
	/*
	  Copy all my range data into the flush data range linked list. If 
	  a range has no keys, still copy it to the flush data list, but 
	  delete it from my range data list.
	*/
	range_ptr = NULL;
	curRange = fd->range_data.range_list;
	for(indx = 0; indx < fd->range_data.num_ranges; indx++){
	  
	  PRINT_FLUSH_DEBUG ("Rank %d getCommands: Looking at range %d with min %s and start range %ld is dirty = %d.\n", fd->mdhim_rank, indx, curRange->range_min, curRange->range_start, curRange->dirty_range);
	  
	  if(curRange->dirty_range){
	    rc = createAndCopyNode(&(fd->flush_list.range_list), curRange->range_start, curRange);
	    if(rc < 0){
	      printf("Rank %d getCommands: Error - Problem creating flush list.\n", fd->mdhim_rank);
	      err = MDHIM_ERROR_BASE;
	    }
	    fd->flush_list.num_ranges += 1 - rc;
	    curRange->dirty_range = 0;
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty. flush list has %d ranges. \n", fd->mdhim_rank, indx, fd->flush_list.num_ranges);

	  }
	  /*
	    Delete ranges with no records from the range list; not from the 
	    flush list yet.
	  */
	  if(curRange->num_records == 0){
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty and has %d records. Going to delete. \n", fd->mdhim_rank,indx, curRange->num_records);
	    deleteRange(fd->range_data.range_list, range_ptr, curRange);
	  }
	  else{
	    range_ptr = curRange;
	    curRange = curRange->next_range;
	  }
	}
	
	/*
	  Now collect range data from all other range servers.
	*/
	if( (tempRangeData = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag))) == NULL){
	  printf("Rank %d getCommands: Error - Unable to allocate memory for the temporary array of range server data.\n", fd->mdhim_rank);
	  err = MDHIM_ERROR_MEMORY;
	}
	
	for(i = 1; i < fd->rangeSvr_size; i++){
	  
	  for(j = 0; j < intArray[i]; j++){
	    if( MPI_Recv(&tempRangeData, sizeof(struct rangeDataTag), MPI_CHAR, fd->range_srv_info[i].range_srv_num, SRVTAG, fd->mdhim_comm, &status) != MPI_SUCCESS){
	      printf("Rank %d getCommands: Error - Unable to receive range server data from process %d.\n", fd->mdhim_rank, fd->range_srv_info[i].range_srv_num);
	      err = MDHIM_ERROR_BASE;
	    }

	    /*
	      Insert the new range into the flush linked list
	    */
	    rc = createAndCopyNode(&(fd->flush_list.range_list), tempRangeData->range_start, tempRangeData);
	    
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: copied range with start %ld and min %s to flush range list\n", fd->mdhim_rank, tempRangeData->range_start, tempRangeData->range_min);
	  }
	}
	
	printList(fd->flush_list.range_list);
	
	free(intArray);
	free(tempRangeData);
      }
      else{
	/*
	  All other procs send your range data to server with rank 0
	*/
	
	range_ptr = NULL;
	curRange = fd->range_data.range_list;
	for(i=0; i < fd->range_data.num_ranges; i++){
	  if(curRange->dirty_range){
	    if (MPI_Send(curRange, sizeof(struct rangeDataTag), MPI_BYTE, fd->range_srv_info[0].range_srv_num, SRVTAG, fd->mdhim_comm) != MPI_SUCCESS){
	      printf("Rank %d getCommands: Error - Unable to send range server data to process %d.\n", fd->mdhim_rank, fd->range_srv_info[0].range_srv_num);
	      err = MDHIM_ERROR_BASE;
	    }
	    curRange->dirty_range = 0;
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty. flush list has %d ranges. \n", fd->mdhim_rank, i, fd->flush_list.num_ranges);
	  }
	  
	  /*
	    Delete ranges with no records from the range list; not from the 
	    flush list yet.
	  */
	  if(curRange->num_records == 0){
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty and has %d recoreds. Going to delete. \n", fd->mdhim_rank,indx, curRange->num_records);
	    deleteRange(fd->range_data.range_list, range_ptr, curRange);
	  }
	  else{
	    range_ptr = curRange;
	    curRange = curRange->next_range;
	  }
	}
	
      }/* end else */
      
      /*
	Send the error code to the requesting process
      */
      MPI_Isend(&err, 1, MPI_INT, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
    }
    else if(!strncmp(recv_buf, "get", 3)){
      /*      
	      At this point, we should only see next and previous requests. 
	      All other possible "get" options were taken care of with flush 
	      data in mdhimGet
      */
      PRINT_GET_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;
      
      /*
	Unpack the get command message. First is "get", then the start range, 
	key index to search on, type of get comparison, current record number.
      */
      sscanf(recv_buf, "%*s %d %d %d %d", &start_range, &parse_int1, &parse_int2, &recordNum);
      memset(recv_buf, '\0', 2048);
      
      indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
      
      PRINT_GET_DEBUG ("Rank %d getCommands: Going to call dbGetKey with isam file indx = %d, key_indx = %d, get type %d recordNum = %d\n", fd->mdhim_rank, indx, parse_int1, parse_int2, recordNum);
      
      if(parse_int2 == MDHIM_NXT){
	parse_int2 = recordNum;
	err = dbGetKey(isamfds[indx], parse_int2, 1, parse_int1, recv_buf, &out_int, &recordNum);
      }
      else if(parse_int2 == MDHIM_PRV){
	parse_int2 = recordNum;
	err = dbGetKey(isamfds[indx], parse_int2, -1, parse_int1, recv_buf, &out_int, &recordNum);
      }


      PRINT_GET_DEBUG ("Rank %d getCommands: Going to send got key %s, return code %d, found key len %d and record number %d.\n", fd->mdhim_rank, recv_buf, err, out_int, recordNum);
      
      /*
	Now pack and send the error code, the new current record number, 
	length of found key and the key found to the requesting 
	process. The send can be non-blocking since this process doesn't 
	need to wait to see or act on if the send is received.
      */
      if( (databuf = (char *)malloc( out_int + 15)) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the output key to get.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      
      memset(databuf, '\0', sizeof(databuf));
      if(sprintf(databuf, "%d %d %d %s", err, recordNum, out_int, recv_buf) < 0){
	printf("Rank %d getCommands: Error - problem packing output results for get\n", fd->mdhim_rank);
      }
      
      PRINT_GET_DEBUG ("Rank %d: THREAD Get - going to send output %s with size %d to source with rank %d\n", fd->mdhim_rank, databuf, (int)strlen(databuf), status.MPI_SOURCE);
      
      MPI_Isend(databuf, strlen(databuf), MPI_BYTE, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
      PRINT_GET_DEBUG ("Rank %d: THREAD Get - Returned from sending done to %d. DONE with FIND\n", fd->mdhim_rank, status.MPI_SOURCE);

      free(databuf);      

    }
    else if(!strncmp(recv_buf, "insert", strlen("insert"))){
      /*
	The insert string may contain multiple records. The insert message 
	looks like "insert start_range key_1_length key_1 
	key_2_length key_2 ... key_numkeys_length key_numkeys data_length data 
	[ ... start_range key_1_length key_1 key_2_length key_2 ... 
	key_numkeys_length key_numkeys data_length data]"
      */
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;
      
      if( (keyLen = (int *)malloc(sizeof(int) * fd->nkeys)) == NULL){
	printf("Rank %d getCommands: Error - Problem allocating memory for the array of key lengths.\n", fd->mdhim_rank);
	err =  MDHIM_ERROR_MEMORY;
      }
      if( (keybuf = (char *)malloc(sizeof(char) * (fd->nkeys + 1) * KEYSIZE)) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the array of keys.\n", fd->mdhim_rank);
	err =  MDHIM_ERROR_MEMORY;
      }
      if( (databuf = (char *)malloc(sizeof(char) * DATABUFFERSIZE)) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the record data.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Allocated keyLen of size %d and keybuf size %d\n", fd->mdhim_rank, fd->nkeys, (int)sizeof(keybuf));
      
      /*
	Get the max number of inserts and the start range for the first insert 
	and enter loop for remaining inserts
      */
      memset(recv_output, '\0', DATABUFFERSIZE);
      p = &(recv_buf[sizeof("insert")]);

      sscanf(p, "%s ", recv_output);
      len = strlen(recv_output) + 1;
      max_ops = atoi(recv_output);
      p += len;

      if( (errArray = (int *)malloc(sizeof(int) * max_ops)) == NULL){
	printf("Rank %d getCommands: Error - Problem allocating memory for the array of insert errors.\n", fd->mdhim_rank);
	err =  MDHIM_ERROR_MEMORY;
      }
      PRINT_INSERT_DEBUG ("Rank %d: After skip p=%s.\n", fd->mdhim_rank, p);
      sscanf(p, "%s ", recv_output);
      len = strlen(recv_output) + 1;
      start_range = atoi(recv_output);
      p += len;
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Pointer to data p=%s and start range %d\n", fd->mdhim_rank, p, start_range);
      
      out_int = 0; // Number of inserts
      do_work = 1;
      while( do_work ){
	
	memset(keybuf, '\0', sizeof(char) * (fd->nkeys + 1) * KEYSIZE);
	
	/*
	  For the primary key and each secondary key, get the size and key 
	  pairs. After the keys is the data length and data. 
	  Place the keys and keylengths into separate arrays. 
	*/
	totalLen = 0;
	for(i = 0; i < fd->nkeys; i++){
	  memset(recv_output, '\0', DATABUFFERSIZE);
	  sscanf(p, "%s ", recv_output);
	  len = strlen(recv_output) + 1;
	  keyLen[i] = atoi(recv_output);
	  p += len;
	  
	  PRINT_INSERT_DEBUG ("Rank %d: THREAD Pointer to data p=%s and key len %d is %d\n", fd->mdhim_rank, p, i, keyLen[i]);
	  
	  len = keyLen[i] + 1;
	  strncpy(&(keybuf[totalLen]), p, len);
	  totalLen += len;
	  p += len;
	  
	  PRINT_INSERT_DEBUG ("Rank %d: For key %d: p =%s totalLen = %d keybuf=%s\n", fd->mdhim_rank, i, p, totalLen, keybuf);
	}
	
	PRINT_INSERT_DEBUG ("Rank %d: Total len is %d and strlen = %d\n", fd->mdhim_rank, totalLen, (int)strlen(keybuf));
	PRINT_INSERT_DEBUG ("Rank %d: p is %s with length %d\n", fd->mdhim_rank, p, (int)strlen(p));
	
	memset(recv_output, '\0', DATABUFFERSIZE);
	sscanf(p, "%s ", recv_output);
	len = strlen(recv_output) + 1;
	p += len;
	len = atoi(recv_output);
	
	memset(databuf, '\0', DATABUFFERSIZE);
	strncpy(databuf, p, len);
	p += len;
	PRINT_INSERT_DEBUG ("Rank %d: THREAD Insert - key buffer=%s data buffer=%s p=%s\n", fd->mdhim_rank, keybuf, databuf, p);
	
	/* 
	   For each insert command, check if file is already open.
	   
	   The directory containing all the key and data files should exist.
	   Check if the range (key and data) file is already open by checking 
	   the correspondng bit in the bit vector; 1 means file exists and is 
	   open. We assume if the data file exists, then all key files exist and 
	   are already open. If the file does not exist, open the files and if 
	   necessary, set the compare function. 	
	*/
	indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
	tempULong = 1 << indx;
	
	PRINT_INSERT_DEBUG ("Rank %d: Range indx = %d bitVector is %lu and shifted by indx %d is %lu\n", fd->mdhim_rank, indx, bitVector, indx, tempULong);
	
	if( !(tempULong & bitVector)){
	  PRINT_INSERT_DEBUG ("Rank %d: File is NOT already open. Open file and set bitVector (%lu).\n", fd->mdhim_rank, bitVector);
	  
	  /*
	    Check if you are creating an ISAM fd beyond the end of the array
	    If so, realloc the array. Since this is a new file, allocate memory 
	    for the ISAM fd.
	  */
	  if(indx > isamfd_counter){
	    // For now, fail if isamfd_counter is not enough. 
	    // Later, make this a b-tree or linked list.
	    
	    printf("Rank %d getCommands: Error - exceeding the number of ISAM files allowed (%d). \n", fd->mdhim_rank, isamfd_counter);
	    MPI_Abort(MPI_COMM_WORLD, 10);
	    
	  }
	  
	  if( (isamfds[indx] = (pblIsamFile_t *)malloc(sizeof(pblIsamFile_t)) ) == NULL){
	    fprintf(stderr, "Rank %d getCommands: Error - Unable to allocate memory for the ISAM fd at index %d.\n", fd->mdhim_rank, indx);
	    err =  MDHIM_ERROR_MEMORY;
	  }
	  
	  if( (dataname = (char *)malloc(sizeof(char) * (strlen(fd->path) + 16) )) == NULL){
	    fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the array of data file name.\n", fd->mdhim_rank);
	    err =  MDHIM_ERROR_MEMORY;
	  }
	  memset(dataname, '\0', sizeof(char) * (strlen(fd->path) + 16));
	  sprintf(dataname, "%sdataRange%d", fd->path, start_range);
	  PRINT_INSERT_DEBUG ("Rank %d: Going to try and open file name %s\n", fd->mdhim_rank, dataname);
	  
	  if( (filenames = (char **)malloc(sizeof(char *) * (fd->nkeys + 1))) == NULL){
	    printf("Rank %d getCommands: Error - Problem allocating memory for the array of key file names.\n", fd->mdhim_rank);
	    err =  MDHIM_ERROR_MEMORY;
	  }
	  
	  for(i = 0; i < fd->nkeys; i++){
	    if( (filenames[i] = (char *)malloc(sizeof(char) * 25)) == NULL){
	      fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the array of key file name %d.\n", fd->mdhim_rank, i+1);
	      err =  MDHIM_ERROR_MEMORY;
	    }
	    memset(filenames[i], '\0', sizeof(char) * 25);
	    sprintf(filenames[i], "Range%dKey%d", start_range, i);
	    PRINT_INSERT_DEBUG ("Rank %d: filenames[%d] = %s\n", fd->mdhim_rank, i, filenames[i]);
	  }
	  
	  fileSettag = NULL;
	  err = MDHIM_SUCCESS;
	  err = dbOpen(&(isamfds[indx]), dataname, fd->update, fileSettag, fd->nkeys, filenames, fd->keydup);
	  
	  PRINT_INSERT_DEBUG ("Rank %d: returned from dbOpen with error = %d.\n", fd->mdhim_rank, err);

	  /*
	    If necessary, set the ISAM setcompare function
	  */
	  //XXX	err = isamSetCompareFunction(&isam, 1, fd->pkey_type);
	  
	  for(i = 0; i < fd->nkeys; i++)
	    free(filenames[i]);
	  free(filenames);
	  free(dataname);
	  
	  /*
	    Set the bit vector to reflect that the file with indx is open
	  */
	  bitVector |= tempULong;
	  PRINT_INSERT_DEBUG ("Rank %d: After open, bitVector is %lu and 2^%d is %lu\n", fd->mdhim_rank, bitVector, indx, tempULong);
	  
	}
	
	/*
	  If no errors have occurred, insert the record into the data store 
	*/
	
	err = MDHIM_SUCCESS;
	if(isamfds[indx] != NULL){
	  PRINT_INSERT_DEBUG ("Rank %d: Going to call isamInsert with nkeys %d, keybuf = %s, databuf = %s\n", fd->mdhim_rank, fd->nkeys, keybuf, databuf);
	  
	  errArray[out_int] = isamInsert(isamfds[indx], fd->nkeys, keyLen, keybuf, databuf, &recordNum);
	  
	  PRINT_INSERT_DEBUG ("Rank %d: isamInsert record number %d return code = %d\n", fd->mdhim_rank, recordNum, errArray[out_int]);
	}
	else{
	  
	  printf("Rank %d: An error has occurred and the insert of keys %s and data %s was aborted. Please try again.\n", fd->mdhim_rank, keybuf, databuf);
	}
	
	PRINT_INSERT_DEBUG ("Rank %d: returned from isamInsert with error = %d.\n", fd->mdhim_rank, err);
	/*
	  Now that the insert completed, we need to update, extend or create 
	  the array of range information.
	*/
	
	if(errArray[out_int] == MDHIM_SUCCESS){
	  PRINT_INSERT_DEBUG ("Rank %d: Adding to the range list of size %d\n", fd->mdhim_rank, fd->range_data.num_ranges);
	  /*
	    curRange = fd->range_data.range_list;
	    for(i=0; i < fd->range_data.num_ranges; i++){
	    printf("Rank %d: Before insert, Range %d with start range %d and num_records %d\n", fd->mdhim_rank, i, curRange->range_start, curRange->num_records);
	    curRange = curRange->next_range;
	    }
	  */
	  if( (indx = searchInsertAndUpdateNode(&(fd->range_data.range_list), start_range, keybuf, keyLen[0], fd->pkey_type)) != -1){
	    fd->range_data.num_ranges += 1 - indx;
	  }
	  else{
	    printf("Rank %d: An error has occurred and the insert of the range was aborted. Range data may not accurately reflect contents of data store.\n", fd->mdhim_rank);
	  }
	  
	  PRINT_INSERT_DEBUG ("Rank %d: Done adding to the range list of size %d.\n", fd->mdhim_rank, fd->range_data.num_ranges);
	}
	
	// XXX Comment out when timing
	curRange = fd->range_data.range_list;
	for(i=0; i < fd->range_data.num_ranges; i++){
	  PRINT_INSERT_DEBUG ("Rank %d: After insert, Range %d with start range %ld, num_records %d, range min %s and range max %s\n", fd->mdhim_rank, i, curRange->range_start, curRange->num_records, curRange->range_min, curRange->range_max );
	  curRange = curRange->next_range;
	}
	
	/*
	  Look for another insert command by looking for the next start range.
	*/
	out_int++;
	memset(recv_output, '\0', DATABUFFERSIZE);
	len = sscanf(p, "%s ", recv_output);

	if(len != 1){
	  do_work = 0;
	}
	else{
	  len = strlen(recv_output) + 1;
	  start_range = atoi(recv_output);
	  p += len;
	  PRINT_INSERT_DEBUG ("Rank %d: DO MORE WORK! Pointer to data p=%s and start range %d\n", fd->mdhim_rank, p, start_range);
	}
	
      }
      
      PRINT_INSERT_DEBUG ("Rank %d: FINAL key buffer=%s data buffer=%s p=%s\n", fd->mdhim_rank, keybuf, databuf, p);
      /*
	Send the ISAM insert error code to requesting process. The send can 
	be non-blocking since this process doesn't need to wait to see or 
	act on if the send is received.
      */
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Insert - going to send error code %d to %d\n", fd->mdhim_rank, err, status.MPI_SOURCE);

      MPI_Isend(errArray, out_int, MPI_INT, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Insert - Returned from sending done to %d. DONE with INSERT\n", fd->mdhim_rank, status.MPI_SOURCE);
      
      free(keybuf);
      free(databuf);
      //XXX Make sure errors were received before freeing error array
      receiveReady(&op_request, MPI_STATUS_IGNORE);
      free(errArray);
    }
    else if (!strncmp(recv_buf, "open", strlen("open"))){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;

      /*
	(Pre)Allocate memory for the array of ISAM file descriptor pointers.
      */
      isamfd_counter = 100;
      if( (isamfds = (pblIsamFile_t **)malloc(sizeof(pblIsamFile_t *) * isamfd_counter)) == NULL){
	printf("Rank %d getCommands: Error - Unable to allocate memory for the array of range server data.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      
      MPI_Isend(&err, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &op_request);

    }
    else if (!strcmp (recv_buf, "quit")){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      /*
	Send the message acknowledging the quit message then exit.
       */
      dowork=0;
      MPI_Isend("quit", strlen("quit"), MPI_CHAR, fd->mdhim_rank, QUITTAG, fd->mdhim_comm, &op_request);
      pthread_exit(&err);
    }
    else{
      //XX Problem with this if an error occurs. You will wait for the irecv to complete forever or something like that. Check this with sending bad command to this routine.
      printf("Rank %d - ERROR: Unrecognized command. The following command will be ignored: %s\n", fd->mdhim_rank, recv_buf);
    }

    receiveReady(&op_request, &status);
  }
  
  return 0;
}

/* ========== spawn_mdhim_server ==========
   Spawns a thread running get_mdhim_commands

   fd is the input MDHIM fd struct with information on range servers

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_SPAWN_SERVER on failure   
*/
int spawn_mdhim_server (MDHIMFD_t * fd){
  int err = MDHIM_SUCCESS;
  pthread_t threads; 
  
  PRINT_MDHIM_DEBUG ("Rank %d: Entered spawn_mdhim_server\n",fd->mdhim_rank);
  
  err = pthread_create(&threads, NULL, getCommands, (void *)(fd));
  if (err) {
    fprintf(stderr, "Pthread create error while spawning server thread: %d\n", err);
    err = MDHIM_ERROR_SPAWN_SERVER;
  }
  
  return err;
}

/* ========== mdhimClose ==========
   Close all open data and key files 

   mdhimClose is a collective call, all MDHIM process must call this routine

   Warning: The order of the pblIsamFile_t pointer array may not be the same 
   order of the final range_list because ranges are inserted into the 
   range_list and can move. The elements in the isam pointer array do not 
   track those moves. 

   Input
   fd is the MDHIM structure containing information on range servers and keys
   
   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) or pbl_error on failure
*/
int mdhimClose(MDHIMFD_t *fd) {

  int err, close_error = 0;
  char data[2048];
  MPI_Request close_request, error_request;
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Entered mdhimClose ****************\n", fd->mdhim_rank);
  
  /*
    Close is a collective call. So, wait for all process to get here.
  */
  MPI_Barrier(fd->mdhim_comm); 

  /*
    Check input parameters
  */
  if(!fd){
    printf("mdhimClose Error - MDHIM FD structure is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /*
    If I"m a range server, send the command "close" 
  */
  if(fd->range_srv_flag){
    memset(data, '\0', 2048);
    strcpy(data, "close");

    /*
      Post a non-blocking received for the error codes from the close command 
      before sending data. This is just to help with deadlocking on send and 
      receives when you are sending to a range server thread that is your child.
    */

    close_error = 0;
    err = MPI_Irecv(&close_error, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &error_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d: mdhimClose ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      Send the close command
    */
    
    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->mdhim_rank, SRVTAG, fd->mdhim_comm, &close_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimClose: ERROR - MPI_Send of close instruction failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    if(close_error > MDHIM_SUCCESS){
      fprintf(stderr, "Rank %d: ERROR -  Problem closing file with return error code %d\n", fd->mdhim_rank, close_error);
    }
    
    PRINT_MDHIM_DEBUG ("Rank %d: Close error code = %d.\n", fd->mdhim_rank, close_error);
  }
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Leaving mdhimClose ****************\n", fd->mdhim_rank);
  
  return close_error;
}
  
/* ========== mdhimDelete ==========
   Delete a record

   Input:
   fd is the MDHIM structure containing information on range servers and keys
   keyIndx is the key to delete; 1 = primary key, 2 = secondary key, etc.
   ikey is the value of the key to delete

   Output:
   record_num is absolute record number of new current key
   okey is the output buffer for the new current key. Memory should be allocated for the okey prior to calling mdhimDelete.
   okey_len is the length of the output key

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimDelete(MDHIMFD_t *fd, int keyIndx, void *ikey, int *record_num, void *okey, int *okey_len) {

  int key_type = -1, ikey_len = 0;
  int rc, i, indx, found = 0,  err = MDHIM_SUCCESS; 
  int server = -1, start_range = -1;
  char *data = NULL;
  char data_buffer[DATABUFFERSIZE];

  struct rangeDataTag *cur_flush, *prev_flush;
  MPI_Request delete_request, error_request;

  PRINT_DELETE_DEBUG ("****************Rank %d Entered mdhimDelete ****************\n", fd->mdhim_rank);
  /*
    Check that the input parameters are valid.
  */
  if(!fd){
    printf("Rank X mdhimDelete: Error - MDHIM fd structure is null.");
    return MDHIM_ERROR_INIT;
  }
  if(!ikey){
    printf("Rank %d mdhimDelete: Error - Input key to delete is null.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if( (keyIndx > fd->nkeys) || (keyIndx < 1)){
    printf("Rank %d mdhimDelete: Error - The input key index %d must be a value from one to the number of keys %d.\n", fd->mdhim_rank, keyIndx, fd->nkeys);
    return MDHIM_ERROR_INIT;
  }

  /*
    Make sure there is data to delete. If there is no flush data, then 
    return with a not found error.
  */
  if(fd->flush_list.num_ranges == 0){
    printf("Rank %d mdhimDelete: Error - There is no global range data. Please flush before calling delete.\n", fd->mdhim_rank);
    return MDHIM_ERROR_NOT_FOUND;
  }
  
  /*
    Find the start range for the input key
  */
  ikey_len = (int)strlen(ikey);
  
  if(keyIndx == 1)
    key_type = fd->pkey_type;
  else
    key_type = fd->alt_key_info[keyIndx - 2].type;
  
  if ( (start_range = whichStartRange(ikey, key_type, fd->max_recs_per_range)) < 0){
    printf("Rank %d mdhimDelete: Error - Unable to find start range for key %s. Record with key %s does not exist.\n", fd->mdhim_rank, (char *)ikey, (char *)ikey);
    return MDHIM_ERROR_KEY;
  }
  
  /*
    Find the index in the flush data range list for the start range. Based on 
    flush information, check if keys exist in range, if the delete key is 
    smaller or larger than the min or max, respectively.
  */
  //XXX This may be dangerous, relying on flush data to see if a key exists. Flush data is not kept up to date after a delete or insert, only on flush.
  
  cur_flush = NULL;
  rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
  if(cur_flush == NULL){
    printf("Rank %d mdhimDelete: Error - Unable to find index of start range for key %s in flushed range data list.\n", fd->mdhim_rank, (char *)ikey);
    return MDHIM_ERROR_IDX_RANGE;
  }
  
  if( (cur_flush->num_records == 0) || 
      (compareKeys(ikey, cur_flush->range_min, ikey_len, key_type) < 0 ) || 
      (compareKeys(ikey, cur_flush->range_max, ikey_len, key_type) > 0) ){
    printf("Rank %d mdhimDelete: Error - Unable to find key %s to delete.\n", fd->mdhim_rank, (char *)ikey);
    return MDHIM_ERROR_NOT_FOUND;
  }
  
  /*
    Since the key may exist, compose and send the delete message
  */
  if( (server = whichServer(start_range, fd->max_recs_per_range, fd->rangeSvr_size)) < 0){
    printf("Rank %d: mdhimDelete Error - Can't find server for key %s.\n", fd->mdhim_rank, (char *)ikey);
    return MDHIM_ERROR_BASE;
  }
  
  /*
    We need to send the delete command, key index and key in a
    single message so that messages to the range servers don't get intermingled.
  */
  if( (data = (char *)malloc(ikey_len + 16)) == NULL){
    printf("Rank %d mdhimDelete: Error - Unable to allocate memory for the find message to send to the range server %d.\n", fd->mdhim_rank, server);
    return MDHIM_ERROR_MEMORY;
  }
  memset(data, '\0', ikey_len+16);
  sprintf(data, "delete %d %d %s", start_range, keyIndx - 1, (char *)ikey);
  
  PRINT_DELETE_DEBUG ("Rank %d: Input (char) key is %s with size %d\n", fd->mdhim_rank, (char *)ikey, ikey_len);
  PRINT_DELETE_DEBUG ("Rank %d: Data buffer is %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
  
  /*
    Post a non-blocking receive for any error codes
  */
  memset(data_buffer, '\0', DATABUFFERSIZE);
  
  if (MPI_Irecv(data_buffer, 2048, MPI_CHAR, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &error_request) != MPI_SUCCESS){
    fprintf(stderr, "Rank %d mdimDelete: ERROR - MPI_Irecv request for delete failed.\n", fd->mdhim_rank);
    return MDHIM_ERROR_COMM;
  }
  
  /*
    Now send the delete request
  */
  PRINT_DELETE_DEBUG ("Rank %d: Sending data buffer %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
  
  if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->range_srv_info[server].range_srv_num, SRVTAG, fd->mdhim_comm, &delete_request) != MPI_SUCCESS){
    fprintf(stderr, "Rank %d mdhimDelete: ERROR - MPI_Send of delete command failed.\n", fd->mdhim_rank);
    return MDHIM_ERROR_COMM;
  }
  
  /*
    Now poll until the non-blocking receive returns.
  */
  receiveReady(&error_request, MPI_STATUS_IGNORE);
  
  /*
    Unpack the returned message with the delete error codes. Delete also 
    sends back the current key and record number.
  */
  PRINT_DELETE_DEBUG ("Rank %d mdhimDelete: Returned the data buffer: %s\n", fd->mdhim_rank, data_buffer);
  
  sscanf(data_buffer, "%d %d %d %s", &err, record_num, okey_len, (char *)okey);
  
  PRINT_DELETE_DEBUG ("Rank %d mdhimDelete: Returned error code %d and current key %s with record number %d.\n", fd->mdhim_rank, err, (char *)okey, *record_num);
  
  strcpy(fd->last_key, okey);
  fd->last_recNum = *record_num;

  PRINT_DELETE_DEBUG ("Rank %d mdhimDelete: Leaving MdhimDelete with last_key =  %s and last_recNum = %d\n", fd->mdhim_rank, fd->last_key, fd->last_recNum);
  free(data);
  
  PRINT_DELETE_DEBUG ("****************Rank %d Leaving mdhimDelete ****************\n", fd->mdhim_rank);

  return err;
}

/* ========== mdhimFinalize ==========
   A collective call to write all data to storage, shut down threads, 
   close files and, if necessary, flush the range data

   fd is the MDHIM structure containing information on range servers and keys
   flushFlag is 1 to flush and 0 otherwise
   
   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimFinalize(MDHIMFD_t *fd, int flushFlag) {
  
  char recv_buffer[10];
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Entered mdhimFinalize ****************\n", fd->mdhim_rank);
  /*
    Check input parameters
  */
  if(!fd){
    printf("mdhimFinalize Error - MDHIM FD structure is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Barrier here to make sure all processes are here
     Then, if necessary, flush data to all MDHIM processes 
  */
  MPI_Barrier(fd->mdhim_comm); 
  
  if(flushFlag){
    
    PRINT_MDHIM_DEBUG ("Rank %d: I want to flush the data flush flag = %d\n", fd->mdhim_rank, flushFlag);
    
    if(mdhimFlush(fd) != 0){
      printf("Rank %d mdhimFinalize: Error - Problem flushing data.\n", fd->mdhim_rank);
      return MDHIM_ERROR_BASE;
    } 
  }

  /*
    Now, close all open data files
  */
    mdhimClose(fd);
  
  /* 
     Send message to have all range server threads stop looking for 
     instructions and exit.
     The blocking receive is there to stop with potential race conditions; 
     the process may get to MPI_Finalize before the thread closes.
  */
  if(fd->range_srv_flag){
    PRINT_MDHIM_DEBUG ("Rank %d: mdhimFinalize Sending quit to mdhim_rank %d\n", fd->mdhim_rank, fd->mdhim_rank);
    MPI_Send("quit", strlen("quit"), MPI_CHAR, fd->mdhim_rank, SRVTAG, fd->mdhim_comm);
    PRINT_MDHIM_DEBUG ("Rank %d: mdhimFinalize before MPI_Recv.\n", fd->mdhim_rank);
    
    memset(recv_buffer, '\0', 10);
    
    MPI_Recv(&recv_buffer, sizeof(recv_buffer), MPI_CHAR, fd->mdhim_rank, QUITTAG, fd->mdhim_comm, MPI_STATUS_IGNORE);

    PRINT_MDHIM_DEBUG ("Rank %d: mdhimFinalize after MPI_Recv.\n", fd->mdhim_rank);
  }
  
  PRINT_MDHIM_DEBUG ("Rank %d: In mdhimFinalize before final MPI_Barrier.\n", fd->mdhim_rank);
  MPI_Barrier(fd->mdhim_comm);
  PRINT_MDHIM_DEBUG ("Rank %d: In mdhimFinalize after final MPI_Barrier.\n", fd->mdhim_rank);
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Leaving mdhimFinalize****************\n", fd->mdhim_rank);
  return MDHIM_SUCCESS;
}

/* ========== mdhimFind ==========
   Find a record in the data stores
   
Input
   fd is the MDHIM structure containing information on range servers and keys
   keyIndx is the key to apply the find operation to; 1 = primary key, 2 = secondary key, etc.
   type is what key to find. Valid types are MDHIM_EQ, MDHIM_EQF, MDHIM_EQL, MDHIM_GEF, MDHIM_GTF, MDHIM_LEL, and MDHIM_LTL
   ikey is the value of the key to find

Output
   record_num is absolute record number returned by the data store
   okey is the output buffer for the key found. Memory should be allocated for the okey prior to calling mdhimFind.
   okey_len is the length of the output key

   Returns: MDHIM_SUCCESS on success, or mdhim_errno (>= 2000) on failure
*/
int mdhimFind( MDHIMFD_t *fd, int keyIndx, int ftype, void *ikey, int *record_num, void *okey, int *okey_len){
  
  int key_type = -1, ikey_len = 0;
  int rc, i, indx, found = 0,  err = MDHIM_SUCCESS; 
  int server = -1, start_range;
  char *data = NULL;
  char data_buffer[DATABUFFERSIZE];

  struct rangeDataTag *cur_flush, *prev_flush;
  MPI_Request find_request, error_request;
  
  PRINT_FIND_DEBUG ("****************Rank %d Entered mdhimFind ****************\n", fd->mdhim_rank);
  /*
    Check that the input parameters are valid.
  */
  if(!fd){
    printf("Rank X mdhimFind: Error - MDHIM fd structure is null.");
    return(MDHIM_ERROR_INIT);
  }
  if(!ikey){
    printf("Rank %d mdhimFind: Error - Input key to search on is null.\n", fd->mdhim_rank);
    return(MDHIM_ERROR_INIT);
  }
  
  PRINT_FIND_DEBUG ("Rank %d mdhimFind: input type = %d ikey = %s\n", fd->mdhim_rank, ftype, (char *)ikey);
  
  if( (keyIndx > fd->nkeys) || (keyIndx < 1)){
    printf("Rank %d mdhimFind: Error - The input key index %d must be a value from one to the number of keys %d.\n",
	   fd->mdhim_rank, keyIndx, fd->nkeys);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_IDX_RANGE ;
  }
  
  if( (ftype != MDHIM_EQ) && (ftype != MDHIM_EQF) && (ftype != MDHIM_EQL) && 
      (ftype != MDHIM_GEF) && (ftype != MDHIM_GTF) && (ftype != MDHIM_LEL) && 
      (ftype != MDHIM_LTL)){
    printf("Rank %d mdhimFind: Error - Problem finding the key %s; %d is an unrecognized mdhimFind option.\n", fd->mdhim_rank, (char *)ikey, ftype);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_INIT;
  }
  
  /*
    Find the start range for the input key
  */
  ikey_len = (int)strlen(ikey);
  
  if(keyIndx == 1)
    key_type = fd->pkey_type;
  else
    key_type = fd->alt_key_info[keyIndx - 2].type;
  
  if((start_range = whichStartRange(ikey, key_type, fd->max_recs_per_range)) < 0){
    printf("Rank %d mdhimFind: Error - Unable to find start range for key %s.\n", fd->mdhim_rank, (char *)ikey);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_BASE;
  }
  
  PRINT_FIND_DEBUG ("Rank %d mdhimFind: key %s has start range %d\n", fd->mdhim_rank, (char *)ikey, start_range);
  
  /*
    Find the index in the flush data range list for the start range.
    Based on flush information and the find operation, we may be able to 
    answer this query without sending data to the range server or may need 
    to modify the range server to send the request to.
  */
  //XXX This may be dangerous, relying on flush data to see if a key exists. Flush data is not kept up to date after a delete or insert, only on flush.

  cur_flush = NULL;
  prev_flush = NULL;
  rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
  
  if(cur_flush == NULL){
    printf("Rank %d mdhimFind: Error - Unable to find index of start range for key %s in flushed range data.\n", fd->mdhim_rank, (char *)ikey);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_IDX_RANGE;
  }
  else if((ftype == MDHIM_EQ) || (ftype == MDHIM_EQF) || (ftype == MDHIM_EQL)){
    
    if((cur_flush->num_records == 0) || 
       (compareKeys(ikey, cur_flush->range_min, ikey_len, key_type) < 0) || 
       (compareKeys(ikey, cur_flush->range_max, ikey_len, key_type) > 0)){
      printf("Rank %d mdhimFind: Warning - Unable to find key equal to %s.\n", fd->mdhim_rank, (char *)ikey);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_NOT_FOUND;
    }
    
  }
  else if( (ftype == MDHIM_GEF) || (ftype == MDHIM_GTF) ){
    
    if((cur_flush->num_records == 0) || (compareKeys(ikey, cur_flush->range_max, ikey_len, key_type) > 0)){
      
      cur_flush = cur_flush->next_range;
      if(cur_flush == NULL){
	printf("Rank %d mdhimFind: Error - Unable to find key greater than or equal to %s.\n", fd->mdhim_rank, (char *)ikey);
	*record_num = -1;
	*okey_len = 0;
	return MDHIM_ERROR_NOT_FOUND;
      }
      
      strncpy(okey, cur_flush->range_min, KEYSIZE);
      *okey_len = strlen(okey);
      *record_num = 1; // XXX For ISAM, we know the absolute record number starts at 1. Need to change for other DBs
      found = 1;
    }
    
  }
  else if((ftype == MDHIM_LEL) || (ftype == MDHIM_LTL)){
    
    if((cur_flush->num_records == 0) || (compareKeys(ikey, cur_flush->range_min, ikey_len, key_type) < 0)){

      cur_flush = prev_flush;
      if(cur_flush == NULL){
	printf("Rank %d mdhimFind: Error - Unable to find key less than or equal to %s.\n", fd->mdhim_rank, (char *)ikey);
	return MDHIM_ERROR_NOT_FOUND;
      }

      strncpy(okey, cur_flush->range_max, KEYSIZE);
      *okey_len = strlen(okey);
      *record_num = cur_flush->num_records; // XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs
      found = 1;
    }
  }
    
  /* 
     If the key was found with flush information, we can skip finding the 
     server to send to, sending a message to the range server to get the key 
     and parsing the results. 
  */
  if(!found){
    if( (server = whichServer(cur_flush->range_start, fd->max_recs_per_range, fd->rangeSvr_size)) < 0){
      printf("Rank %d mdhimFind: Error - Can't find server for key %s.\n", fd->mdhim_rank, (char *)ikey);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_BASE;
    }
    
    /*
      We need to send the find command, key index, comparison type and key to 
      the range server.
    */
    if( (data = (char *)malloc(ikey_len + 15)) == NULL){
      printf("Rank %d mdhimFind: Error - Unable to allocate memory for the find message to send to the range server %d.\n", fd->mdhim_rank, server);
      *record_num = -1;
      *okey_len = 0;
      err = MDHIM_ERROR_MEMORY;
    }
    memset(data, '\0', ikey_len+15);
    sprintf(data, "find %d %d %d %s", start_range, keyIndx - 1, ftype, (char *)ikey);
    
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Input (char) key is %s with size %d\n", fd->mdhim_rank, (char *)ikey, ikey_len);
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Data buffer is %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
  
    /*
      Post a non-blocking receive for any error codes or the retrieved 
      key/data/record number.
    */
    memset(data_buffer, '\0', DATABUFFERSIZE);
    
    if (MPI_Irecv(data_buffer, 2048, MPI_CHAR, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &error_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdimFind: ERROR - MPI_Irecv request for found key/data failed.\n", fd->mdhim_rank);
    }
    
    /*
      Now send the find request
    */
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Sending data buffer %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
    
    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->range_srv_info[server].range_srv_num, SRVTAG, fd->mdhim_comm, &find_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFind: ERROR - MPI_Send of find data failed.\n", fd->mdhim_rank);
      // XXX what to do if sending of find fails? Probably retry the send. 
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    /*
      Unpack the returned message with the find results. The return string 
      should have an error code, absolute record number, found key length and 
      a string with the key it found.
    */
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Returned the data buffer: %s\n", fd->mdhim_rank, data_buffer);
    
    sscanf(data_buffer, "%d %d %d %s", &err, record_num, okey_len, (char *)okey);
    
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Returned error code %d record num %d and data buffer: %s\n", fd->mdhim_rank, err, *record_num, data);
    
    if(err == MDHIM_ERROR_NOT_FOUND){
      okey = NULL;
      *okey_len = 0;
      *record_num = -1;
    }
    else if(err < 0){
      okey = NULL;
      *okey_len = 0;
      *record_num = -1;
    }
    else{
      strncpy(fd->last_key, okey, KEYSIZE);
      *okey_len = strlen(okey);
      fd->last_key[*okey_len] = '\0';
      fd->last_recNum = *record_num;
    }
    
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Leaving MdhimFind with last_key =  %s and last_recNum = %d\n", fd->mdhim_rank, fd->last_key, fd->last_recNum);
    free(data);
  }
  
  PRINT_FIND_DEBUG ("****************Rank %d Leaving mdhimFind ****************\n", fd->mdhim_rank);
  return err;
}

/* ========== mdhimFlush ==========
   Send all information about data to all processes in the job
   
   mdhimFlush is a collective call, all MDHIM clients participating in the job 
   must call this function. All range servers send range data to the "first" 
   range server. 
   
   fd is the MDHIM structre that range server information and data

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimFlush(MDHIMFD_t *fd) {
  
  int err = MDHIM_SUCCESS, flush_error = MDHIM_SUCCESS;
  int numFlushRanges = 0;
  int rc, server, len, indx; 
  char *key = NULL, *data = NULL;
  struct rangeDataTag *range_ptr, *tempRangeData, *curRange;
  MPI_Request flush_request, error_request;
  
  PRINT_FLUSH_DEBUG ("****************Rank %d Entered mdhimFlush****************\n", fd->mdhim_rank);
  PRINT_FLUSH_DEBUG ("Rank %d: Inside MDHIM FLUSH with spawned thread flag %d.\n", fd->mdhim_rank, fd->range_srv_flag); 
  
  /*
   * Check input parameters
   */
  if(!fd){
    printf("Rank X mdhimFlush: Error - MDHIM fd structure is null.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /*
    Since flush is a collective call, wait for all process to get here. 
    We need to make sure all inserts are complete
  */
  PRINT_FLUSH_DEBUG ("Rank %d: Inside MDHIM FLUSH before barrier\n", fd->mdhim_rank); 
  MPI_Barrier(fd->mdhim_comm);
  PRINT_FLUSH_DEBUG ("Rank %d: Inside MDHIM FLUSH after barrier\n", fd->mdhim_rank); 
  
  /*
    If you're a range server, post a non-blocking received for the error 
    codes from the flush command before sending data. This is just to help 
    with deadlocking on send and receives when you are sending to a range 
    server thread that is your child. 
  */
  if(fd->range_srv_flag){
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Before post of Ireceive for flush error message from %d\n", fd->mdhim_rank, fd->mdhim_rank);
    
    err = MPI_Irecv(&flush_error, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &error_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFlush: ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: I am a range server with %d total ranges.\n", fd->mdhim_rank, fd->range_data.num_ranges);
    
    /*
      Send the flush command
    */
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Posted Ireceive for Flush error message from %d\n", fd->mdhim_rank, fd->mdhim_rank);
    
    err = MPI_Isend("flush", strlen("flush"), MPI_CHAR, fd->mdhim_rank,  SRVTAG, fd->mdhim_comm, &flush_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFlush: ERROR - MPI_Send of flush command failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Sent data to %d successful.\n", fd->mdhim_rank, fd->mdhim_rank);
    
    /*
      Now poll until the non-blocking receive for error code returns. 
      Receiving an error code means the flush has completed.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    if(flush_error > MDHIM_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFlush: ERROR -  Problem flushing with return error code %d\n", fd->mdhim_rank, flush_error);
    }
    err = flush_error;
  }
  
  /*
    Now that one range server has collected range data from all servers, 
    send this data to all processes. 
    Only "dirty" ranges will be sent, i.e. only ranges that changed since the 
    last flush will be sent.
  */
  if(fd->mdhim_rank == fd->range_srv_info[0].range_srv_num){
    //    numFlushRanges = fd->flush_list.num_ranges;
    range_ptr = fd->flush_list.range_list;
    for(indx = 0; indx < fd->flush_list.num_ranges; indx++){
      if(range_ptr->dirty_range)
	numFlushRanges++;
      PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Flush range %d is dirty = %d.\n", fd->mdhim_rank, indx, range_ptr->dirty_range);
      range_ptr = range_ptr->next_range;
    }
    
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Master range server list - number of flush ranges %d number of dirty flush ranges %d.\n", fd->mdhim_rank, fd->flush_list.num_ranges, numFlushRanges);
  }
  
  PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Before bcast number of flush ranges %d and dirty flush ranges %d.\n", fd->mdhim_rank, fd->flush_list.range_list->dirty_range, numFlushRanges);
  
  MPI_Bcast(&numFlushRanges, 1, MPI_INT, fd->range_srv_info[0].range_srv_num, fd->mdhim_comm);
  
  PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: After bcast number of flush ranges %d and dirty flush ranges %d.\n", fd->mdhim_rank, fd->flush_list.range_list->dirty_range, numFlushRanges);
  
  /* 
     If there are no dirty ranges, all the old ranges in the flush list 
     are still valid and we are done.
  */
  if(numFlushRanges == 0){
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: There are no new ranges to flush. There are %d existing ranges.\n", fd->mdhim_rank, fd->flush_list.num_ranges);

    return flush_error;
  }

  /*
    For all MDHIM clients, get the dirty range data from the range server 
    with server rank 0.
   */
  //XXX Start editing here -------------------->
  //  if(fd->mdhim_rank != fd->range_srv_info[0].range_srv_num){
  //fd->flush_list.num_ranges = numFlushRanges;
  //}

  /*
    Broadcast the dirty flush ranges
  */
  PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Before bcast from %d of struct number of flush list ranges %d.\n", fd->mdhim_rank, fd->range_srv_info[0].range_srv_num, fd->flush_list.num_ranges);
  
  if(fd->mdhim_rank == fd->range_srv_info[0].range_srv_num){
    range_ptr = fd->flush_list.range_list;
    
    for(indx=0; indx < fd->flush_list.num_ranges; indx++){
      if(range_ptr->dirty_range){
	
	MPI_Bcast(range_ptr, sizeof(range_ptr), MPI_CHAR, fd->range_srv_info[0].range_srv_num, fd->mdhim_comm);
	PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Master server bcast of range %d with start range %ld.\n", fd->mdhim_rank, indx, range_ptr->range_start);
      }
      range_ptr = range_ptr->next_range;
    }
    
  }
  else{
    if( (tempRangeData = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag))) == NULL){
      printf("Rank %d mdhimFlush: Error - Unable to allocate memory for the temporary array of range server data.\n", fd->mdhim_rank);
      err = MDHIM_ERROR_MEMORY;
    }
    
    range_ptr = fd->flush_list.range_list;
    
    for(indx=0; indx < numFlushRanges; indx++){
      MPI_Bcast(&tempRangeData, sizeof(tempRangeData), MPI_CHAR, fd->range_srv_info[0].range_srv_num, fd->mdhim_comm);
      rc = createAndCopyNode(&(fd->flush_list.range_list), tempRangeData->range_start, tempRangeData);
      fd->flush_list.num_ranges += 1 - rc;
      
      
    }
  }
  /*
    Get rid of any flush ranges that have no records.
  */
  // XX this really needs to be handled better, i.e. the sending of and then deleting of dirty ranges with no records. We do this so that changes can be propogated, but there's a better way.
  curRange = fd->flush_list.range_list;
  range_ptr = NULL;
  for(indx=0; indx < fd->flush_list.num_ranges; indx++){
    
    if(curRange->num_records == 0){
      deleteRange(fd->flush_list.range_list, range_ptr, curRange);
      fd->flush_list.num_ranges--;
    }
    else{
      curRange->dirty_range = 0;
      range_ptr = curRange;
      curRange = curRange->next_range;
    }
  }
  
  PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: After bcast number of flush list ranges %d\n", fd->mdhim_rank, fd->flush_list.num_ranges);
  
  range_ptr = fd->flush_list.range_list;
  for(indx=0; indx < fd->flush_list.num_ranges; indx++){
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Flush list range %d number of records %d start range %ld\n", fd->mdhim_rank, indx, range_ptr->num_records, range_ptr->range_start);
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Flush list range %d min key %s max key %s\n", fd->mdhim_rank, indx, range_ptr->range_min, range_ptr->range_max);
    range_ptr = range_ptr->next_range;
  }
  
  PRINT_FLUSH_DEBUG ("****************Rank %d Leaving mdhimFlush****************\n", fd->mdhim_rank);
  
  return flush_error;
}

/* ========== mdhimGet ==========
   Get the key from the first, last, current, next or previous record
   
   Input:
   fd is the MDHIM structure containing information on range servers and keys
   keyIndx is the key to apply the find operation to; 1 = primary key, 2 = secondary key, etc.
   type is what key to find. Valid choices are first key, last key, previous, next and current key; MDHIM_FXF, MDHIM_LXL, MDHIM_PRV, MDHIM_NXT, and MDHIM_CUR respectively
   record_num is absolute record number that you just found
   
   Output:
   record_num is absolute record number returned by the data store, -1 if record not found
   okey is the output buffer for the key found
   okey_len is the length of the output key, 0 if record not found

   Returns: MDHIM_SUCCESS on success, or mdhim_errno (>= 2000) on failure
*/
int mdhimGet( MDHIMFD_t *fd, int keyIndx, int ftype, int *record_num, void *okey, int *okey_len){
  
  int key_type = -1, ikey_len = 0;
  int rc, i, indx, found = 0,  err = MDHIM_SUCCESS; 
  int server = -1, start_range;
  char *data = NULL;
  char data_buffer[DATABUFFERSIZE];
  
  struct rangeDataTag *cur_flush, *prev_flush;
  MPI_Request get_request, error_request;
  
  PRINT_GET_DEBUG ("****************Rank %d Entering mdhimGet ****************\n", fd->mdhim_rank);

  /*
    Set output variables to some default values in case there's a problem.
  */
  *record_num = -1;
  *okey_len = 0;
  
  /*
    Check that the input parameters are valid.
  */
  if(!fd){
    printf("Rank X mdhimGet: Error - MDHIM fd structure is null in mdhimGet.\n");
    return MDHIM_ERROR_INIT;
  }
  
  PRINT_GET_DEBUG ("Rank %d mdhimGet: input ftype = %d\n", fd->mdhim_rank, ftype);

  PRINT_GET_DEBUG ("Rank %d mdhimGet: fd->flush_list.num_ranges = %d\n", fd->mdhim_rank, fd->flush_list.num_ranges); 
  if(fd->flush_list.num_ranges < 1){
    printf("Rank %d mdhimGet: Error - There is no flush range data.\n", fd->mdhim_rank);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_DB_GET_KEY;
  }

  PRINT_GET_DEBUG ("Rank %d mdhimGet: fd->flush_list.num_ranges = %d num_records = %d range start = %ld\n", fd->mdhim_rank, fd->flush_list.num_ranges, fd->flush_list.range_list->num_records, fd->flush_list.range_list->range_start);
  
  if( (keyIndx > fd->nkeys) || (keyIndx < 1)){
    printf("Rank %d mdhimGet: Error - The input key index %d must be a value from one to the number of keys %d.\n",
	   fd->mdhim_rank, keyIndx, fd->nkeys);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_IDX_RANGE;
  }
  
  if( (ftype != MDHIM_PRV) && (ftype != MDHIM_NXT) && (ftype != MDHIM_CUR) && 
      (ftype != MDHIM_LXL) && (ftype != MDHIM_FXF) ){
    printf("Rank %d mdhimGet: Error - Problem getting the key; %d is an unrecognized mdhimGet option.\n",
	   fd->mdhim_rank, ftype);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_BASE;
  }
  
  /*
    Based on the get operation, we may be able to answer this query without 
    asking the range servers. Check what the get request is and answer 
    with flush data if possible. If not, compose the get message for the range 
    servers.
  */
  if( ftype == MDHIM_FXF){
    indx = 0;

    PRINT_GET_DEBUG ("Rank %d: mdhimGet first key from list is %s\n", fd->mdhim_rank, fd->flush_list.range_list->range_min);
    strncpy(okey, fd->flush_list.range_list->range_min, KEYSIZE);

    *okey_len = strlen(okey);    
    *record_num = 1; //XXX For ISAM, we know the absolute record numbers start with 1. Need to change for other DBs.
    found = 1;

    PRINT_GET_DEBUG ("Rank %d mdhimGet: The first key from list is %s outkey is %s and okey_size %d rec_num %d\n", fd->mdhim_rank, fd->flush_list.range_list->range_min, (char *)okey, *okey_len, *record_num);
  }
  else if( ftype == MDHIM_LXL){
    /* 
       Find the last range
    */
    indx = fd->flush_list.num_ranges - 1;
    cur_flush = fd->flush_list.range_list;
    for(i=0; i < indx; i++){
      cur_flush = cur_flush->next_range;
    }

    strncpy(okey, cur_flush->range_max, KEYSIZE);
    *okey_len = strlen(okey);    
    *record_num = cur_flush->num_records; //XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs.
    found = 1;
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: The last key from list is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, cur_flush->range_max, (char *)okey, *okey_len, *record_num);
    
  }
  else if( ftype == MDHIM_CUR ){
    memset(okey, '\0', KEYSIZE);
    strncpy(okey, fd->last_key, KEYSIZE);
    *okey_len = strlen(okey);
    *record_num = fd->last_recNum;
    found = 1;
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: The current key is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, fd->last_key, (char *)okey, *okey_len, *record_num);
  }
  else{  
    /*
      For get next and get previous, find the start range from the stored 
      last key found
    */
    if((fd->last_key == NULL) || (fd->last_recNum < 0)){
      printf("Rank %d mdhimGet: Error - Unable to get next or previous. Must first get or find a key.\n", fd->mdhim_rank);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_BASE;
    }
    
    ikey_len = (int)strlen(fd->last_key);
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: last_key seen = %s with rec num %d ikey_len = %d\n", fd->mdhim_rank, fd->last_key, fd->last_recNum, ikey_len);

    if(keyIndx == 1)
      key_type = fd->pkey_type;
    else
      key_type = fd->alt_key_info[keyIndx - 2].type;
    
    if ( (start_range = whichStartRange(fd->last_key, key_type, fd->max_recs_per_range)) < 0){
      printf("Rank %d mdhimGet: Error - Unable to find start range for key %s.\n", fd->mdhim_rank, (char *)fd->last_key);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: last key seen %s has start range %d\n", fd->mdhim_rank, fd->last_key, start_range);

    /*
      Find the index in the flush data range list for the start range.
      Based on flush information and the get operation, we may be able to 
      answer this query without sending data to the range server or may need 
      to modify the range server to send the request to.
    */
    indx = -1;
    cur_flush = NULL;
    prev_flush = NULL;
    rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
    
    if(cur_flush == NULL){
      printf("Rank %d mdhimGet: Error - Unable to find index of start range for key %s in flushed range list.\n", fd->mdhim_rank, (char *)fd->last_key);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_IDX_RANGE;
    }
    
    else if( ftype == MDHIM_PRV ){
      
      PRINT_GET_DEBUG ("Rank %d mdhimGet: flush list found start range %d\n", fd->mdhim_rank, start_range);
      
      if((indx == 0) && ( (cur_flush->num_records == 0) || (compareKeys(fd->last_key, cur_flush->range_min, ikey_len, key_type) <= 0))){
	/*
	  The last key seen is in the first flush range and either there are no records in this range or the last key seen is less than or equal to the first record in this range, i.e. the first key. Thus, there is no previous key.
	*/

	printf("Rank %d mdhimGet: Warning - Unable to get previous key to %s because no previous key exists.\n", fd->mdhim_rank, (char *)fd->last_key);
	*record_num = -1;
	*okey_len = 0;
	return MDHIM_ERROR_NOT_FOUND;
      }
      else if((cur_flush->num_records == 0) || ( compareKeys(fd->last_key, cur_flush->range_min, ikey_len, key_type) == 0)){
	cur_flush = prev_flush;
	
	if(cur_flush == NULL){
	  printf("Rank %d mdhimGet: Warning - Unable to get previous key to %s because no previous key exists.\n", fd->mdhim_rank, (char *)fd->last_key);
	  *record_num = -1;
	  *okey_len = 0;
	  return MDHIM_ERROR_NOT_FOUND;
	}
	
	strncpy(okey, cur_flush->range_max, KEYSIZE);
	*okey_len = strlen(okey);
	*record_num = cur_flush->num_records; //XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs
	found = 1;

	PRINT_GET_DEBUG ("Rank %d mdhimGet: The previous key from list is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, cur_flush->range_max, (char *)okey, *okey_len, *record_num);
      }
      
    }
    else if( ftype == MDHIM_NXT ){

      PRINT_GET_DEBUG ("Rank %d mdhimGet: NXT flush_list.num_ranges =  %d cur_flush->num_records %d cur_flush->range_max = %s\n", fd->mdhim_rank, fd->flush_list.num_ranges, cur_flush->num_records, cur_flush->range_max);
      
      if((indx == (fd->flush_list.num_ranges - 1)) && ( (cur_flush->num_records == 0) || (compareKeys(fd->last_key, cur_flush->range_max, ikey_len, key_type) >= 0))){
	/*
	  The last key seen is in the last flush range and either there are no records in this range or the last key seen is greater than or equal to the last record in this range, i.e. the last key. Thus, there is no next key.
	*/

	printf("Rank %d mdhimGet: Error - Unable to get next key to %s because no next key exists in list.\n", fd->mdhim_rank, (char *)fd->last_key);
	*record_num = -1;
	*okey_len = 0;
	return MDHIM_ERROR_NOT_FOUND;
      }
      else if((cur_flush->num_records == 0) || ( compareKeys(fd->last_key, cur_flush->range_max, ikey_len, key_type) == 0)){
	cur_flush = cur_flush->next_range;

	if(cur_flush == NULL ){
	  printf("Rank %d mdhimGet: Error - Unable to get next key to %s because no next key exists in list.\n", fd->mdhim_rank, (char *)fd->last_key);
	  *record_num = -1;
	  *okey_len = 0;
	  return MDHIM_ERROR_NOT_FOUND;
	}
	
	PRINT_GET_DEBUG ("Rank %d mdhimGet: NXT flush_list.range_min =  %s\n", fd->mdhim_rank, cur_flush->range_min);
	
	strncpy(okey, cur_flush->range_min, KEYSIZE);
	*okey_len = strlen(okey);
	*record_num = 1; //XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs
	found = 1;
	PRINT_GET_DEBUG ("Rank %d mdhimGet: The next key from list is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, cur_flush->range_min, (char *)okey, *okey_len, *record_num);
      }
      
    }
  }
  
  /* 
     If the key was found with flush information, we can skip finding the 
     server to send to, sending a message to the range server to get the key 
     and parsing the results. 
  */
  if(!found){
    if( (server = whichServer(cur_flush->range_start, fd->max_recs_per_range, fd->rangeSvr_size)) < 0){
      printf("Rank %d: mdhimGet Error - Can't find server for key %s.\n", fd->mdhim_rank, (char *)fd->last_key);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      We need to send the get command, key index, comparison type and current 
      record number that we want in a single message so that messages to the 
      range servers don't get intermingled.
    */
    if( (data = (char *)malloc(25)) == NULL){
      printf("Rank %d mdhimGet: Error - Unable to allocate memory for the get message to send to the range server %d.\n", fd->mdhim_rank, server);
      err = MDHIM_ERROR_MEMORY;
    }
    memset(data, '\0', 25);
    sprintf(data, "get %d %d %d %d", start_range, keyIndx - 1, ftype, fd->last_recNum);    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Input (char) key is %s with number %d size %d\n", fd->mdhim_rank, (char *)fd->last_key, fd->last_recNum, ikey_len);
    PRINT_GET_DEBUG ("Rank %d mdimGet: Data buffer is %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
    
    /*
      Post a non-blocking receive for any error codes or the retrieved 
      key/data/record number.
    */
    memset(data_buffer, '\0', DATABUFFERSIZE);
    if (MPI_Irecv(data_buffer, 2048, MPI_CHAR, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &error_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdimGet: ERROR - MPI_Irecv request for found key/data failed.\n", fd->mdhim_rank);
    }
    
    /*
      Now send the get request
    */
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Sending data buffer %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->range_srv_info[server].range_srv_num, SRVTAG, fd->mdhim_comm, &get_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimGet: ERROR - MPI_Send of find data failed.\n", fd->mdhim_rank);
      // XXX what to do if sending of get fails? Probably retry the send. 
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);

    /*
      Unpack the returned message with the get results. The return string 
      should have an error code, absolute record number, found key length and 
      a string with the key it found.
    */
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Returned the data buffer: %s\n", fd->mdhim_rank, data_buffer);
    
    sscanf(data_buffer, "%d %d %d %s", &err, record_num, okey_len, (char *)okey);
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Returned error code %d record number %d and found key %s\n", fd->mdhim_rank, err, *record_num, (char *)okey);
    
    if(err != MDHIM_SUCCESS){
      okey = NULL;
      *record_num = -1;
    }
    else{
      strncpy(fd->last_key, okey, *okey_len);
      fd->last_key[*okey_len] = '\0';
      fd->last_recNum = *record_num;
    }

    free(data);
  } /* end if(!found) */
  else{
    strncpy(fd->last_key, okey, *okey_len);
    fd->last_key[*okey_len] = '\0';
    fd->last_recNum = *record_num;
  }
  
  PRINT_GET_DEBUG ("Rank %d mdhimGet: last record number %d and last key %s\n", fd->mdhim_rank, fd->last_recNum, fd->last_key);
  PRINT_GET_DEBUG ("****************Rank %d Leaving mdhimGet****************\n", fd->mdhim_rank);
  return err;
}

/* ========== mdhimInit ==========
   Initialization routine for MDHIM. 
   
   mdhimInit is a collective call, all processes participating in the job 
   must call this function, and it must be called before any other MDHIM 
   routine. Threads on range servers will be started and MDHIM fd 
   structures will be initalized with range server information. 
   
   fd is the MDHIMFD_t structre that will be initalized with range server information
   numRangeSvrs is the number of unique range server hosts. 
   rangeSvrs array of range server (host) names. No duplicate names.
   numRangeSvrsByHost is an array of the number of range servers on each of 
   the hosts in the rangeSvrs array.
   commType is the type of communication between processes; 
   1 is MPI, 2 PGAS (only MPI is currently supported)

   Returns: MDHIM_SUCCESS on success or one of the following on failure 
   MDHIM_ERROR_INIT, MDHIM_ERROR_BASE or MDHIM_ERROR_MEMORY;
*/
int mdhimInit(MDHIMFD_t *fd, int numRangeSvrs, char **rangeSvrs, int *numRangeSvrsByHost, int commType, MPI_Comm inComm ){
  
  char hostName[HOSTNAMELEN];
  
  int i, indx, j;
  int mdhimRank = -1;
  int *rangeNameIndx = NULL;
  int *rangeSvrsByHostDup = NULL;

  MPI_Group mdhim_group, range_svr_group;

  PRINT_INIT_DEBUG ("****************Rank Entered mdhimInit****************\n");

  /*
    Check input parameters
  */
  if(!fd){
    printf("mdhimInit Error - MDHIM FD structure is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  if(commType != 1){
    printf("mdhimInit Error - MPI is the only communication method supported.\n");
    return MDHIM_ERROR_BASE;
  }
  if(!rangeSvrs){
    printf("mdhimInit Error - Array of range server names is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Get information on the MPI job and fill in the MDHIMFD_t struct
  */
  MPI_Comm_rank(inComm, &mdhimRank);	 
  MPI_Comm_size(inComm, &(fd->mdhim_size));
  /*
  MPI_Comm_dup(inComm, &(fd->mdhim_comm));
  */

  fd->rangeSvr_size = 0;
  for(i = 0; i < numRangeSvrs; i++)
    fd->rangeSvr_size += numRangeSvrsByHost[i];
  
  PRINT_INIT_DEBUG ("Rank %d: Number of range servers = %d\n", mdhimRank, fd->rangeSvr_size);

  if(numRangeSvrs < 1 || fd->rangeSvr_size > fd->mdhim_size){
    printf("Rank %d: mdhimInit Error - There must be at least one range server and less than the number of process in the job (%d). " 
	   "Total number of range servers entered was %d.\n", mdhimRank, fd->mdhim_size, fd->rangeSvr_size);
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Let's figure out who the range servers are:
     Everyone reads the input array of host names of the range servers. 
     If your host name is in the array, send to rank zero the index in 
     the range server name list of what host you are on starting with 1 not 0.
  */
  gethostname(hostName, HOSTNAMELEN);
  
  indx = 0;
  for (i = 0; i < numRangeSvrs; i++)
    if(strncmp(hostName, rangeSvrs[i], strlen(rangeSvrs[i])) == 0)
      indx = i+1;

  PRINT_INIT_DEBUG ("Rank %d: My host name is %s found at index %d\n", mdhimRank, hostName, indx);

  if(mdhimRank == 0){
    if( (rangeNameIndx = (int *)malloc(sizeof(int) * fd->mdhim_size)) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server membership array.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
  }

  /*
    Rank 0 gathers info on all procs on if they are on a range server.
    Compute number of occurances of hostname in the range server list.
    Rank 0 then fills in the array of range server information in the 
    MDHIMFD_t struct.
  */
  MPI_Gather(&indx, 1, MPI_INT, rangeNameIndx, 1, MPI_INT, 0, inComm);
  
  if(mdhimRank == 0){    
    if( (rangeSvrsByHostDup = (int *)malloc(numRangeSvrs * sizeof(int))) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server name count.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
    
    for(i = 0; i < numRangeSvrs; i++)
      rangeSvrsByHostDup[i] = numRangeSvrsByHost[i];
    
    if( (fd->range_srv_info = (RANGE_SRV *)malloc(fd->rangeSvr_size * sizeof(RANGE_SRV)) ) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server name count.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
    
    j = 0;
    for(i = 0; i < fd->mdhim_size; i++){
      indx = rangeNameIndx[i];
      
      if(indx > 0){
	indx--;
	if(rangeSvrsByHostDup[indx] > 0){
	  fd->range_srv_info[j].range_srv_num = i; /* rank in mdhim_comm */
	  fd->range_srv_info[j].name = (char *)malloc(HOSTNAMELEN);
	  memset(fd->range_srv_info[j].name, '\0', HOSTNAMELEN);
	  strcpy(fd->range_srv_info[j].name, rangeSvrs[indx]);
	  j++;
	  rangeSvrsByHostDup[indx]--;
	}
      }
    }
    
    /* 
       Check that the total number and number of range servers per host 
       are correct.
    */
    if(j != fd->rangeSvr_size){
      printf("Rank %d: mdhimInit Error - Number of range server assigned (%d) does not equal user supplied number (%d).\n", mdhimRank, j, fd->rangeSvr_size);
      return MDHIM_ERROR_BASE;
    }
    
    for(i = 0; i < numRangeSvrs; i++)
      if(rangeSvrsByHostDup[indx] != 0){
	printf("Rank %d: mdhimInit Error - Range server %s does not have enough procs to be a range server.\n", mdhimRank, rangeSvrs[i]);
	return MDHIM_ERROR_BASE;
      }
    
    free(rangeSvrsByHostDup);
    free(rangeNameIndx);

    /*
      Now initalize the entries on keys and last record seen. Many of these 
      will be set in the open routine.
    */
    fd->mdhim_comm = NULL;
    fd->mdhim_rank = -1;
    fd->max_recs_per_range = 0;
    fd->pkey_type = -1;
    fd->max_pkey_length = 0;    
    fd->max_pkey_padding_length = 0; 
    fd->max_data_length = 0;    
    fd->nkeys = 0;
    fd->update = -1;
    fd->keydup = NULL;
    memset(fd->last_key, '\0', KEYSIZE);
    fd->last_recNum = -1;
    fd->path = NULL;
    fd->alt_key_info = NULL;

    /* 
       Set the range data and flush data to NULL and initalize number of ranges.
    */
    fd->range_data.num_ranges = 0;
    fd->range_data.range_list = NULL;
    fd->flush_list.num_ranges = 0;
    fd->flush_list.range_list = NULL;
  } /* end if(mdhimRank == 0 */
  
  /* 
     Broadcast the MDHIMFD_t struct from rank 0.
  */
  PRINT_INIT_DEBUG ("Rank %d mdhimInit: Before bcast of fd. size of *fd %d size of fd %d\n", mdhimRank, (int)sizeof(*fd), (int)sizeof(fd));
  MPI_Bcast(fd, sizeof(*fd), MPI_BYTE, 0, inComm);

  if(mdhimRank > 0){
    if( (fd->range_srv_info = (RANGE_SRV *)malloc(fd->rangeSvr_size * sizeof(RANGE_SRV)) ) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server name count.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
  }

  MPI_Bcast(fd->range_srv_info, sizeof(RANGE_SRV) * fd->rangeSvr_size, MPI_BYTE, 0, inComm);
  
  PRINT_INIT_DEBUG ("Rank %d mdhimInit: Number of range servers %d\n", mdhimRank, fd->rangeSvr_size);

  for(j=0; j < fd->rangeSvr_size; j++){
    if(mdhimRank > 0){
      fd->range_srv_info[j].name = (char *)malloc(HOSTNAMELEN);
      memset(fd->range_srv_info[j].name, '\0', HOSTNAMELEN);
    }
    MPI_Bcast(fd->range_srv_info[j].name, HOSTNAMELEN, MPI_BYTE, 0, inComm);
    PRINT_INIT_DEBUG ("Rank %d mdhimInit: Hostname %d = %s\n", mdhimRank, j, fd->range_srv_info[j].name);
  }

  /* 
     Fill in rank specific values in the MDHIMFD_t struct
  */
  fd->mdhim_rank = mdhimRank;

  if(MPI_Comm_dup(inComm, &(fd->mdhim_comm)) != MPI_SUCCESS){
    printf("Rank %d mdhimInit: Error - Unable to duplicate the incoming MPI Communicator group.\n", fd->mdhim_rank);
    return MDHIM_ERROR_BASE;
  }
  
  PRINT_INIT_DEBUG ("Rank %d mdhimInit: After MPI_Comm_dup\n", fd->mdhim_rank);

  PRINT_INIT_DEBUG ("Rank %d: Number of range servers %d\n", fd->mdhim_rank, fd->rangeSvr_size);
  
  /*
    Create an MPI Communicator for range servers
  */
  fd->range_srv_flag = 0;
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(fd->mdhim_rank == fd->range_srv_info[i].range_srv_num){
      fd->range_srv_flag = 1;      
    }
  }

  MPI_Comm_split(fd->mdhim_comm, fd->range_srv_flag, fd->mdhim_rank, &(fd->rangeSrv_comm));
  
  /* 
     Start up a thread on each range server to accept data store operations.
  */
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(fd->mdhim_rank == fd->range_srv_info[i].range_srv_num){
      
      if(spawn_mdhim_server(fd) != 0){  
	fprintf( stderr, "Rank %d: mdhimInit Error - Spawning thread failed on host %s with error %s\n", fd->mdhim_rank, hostName, strerror( errno ));
	return MDHIM_ERROR_BASE;
      }
      
      PRINT_INIT_DEBUG ("Rank %d: Done spawning thread.\n", fd->mdhim_rank);
    }
  }

  PRINT_INIT_DEBUG ("****************Rank %d Leaving mdhimInit****************\n", fd->mdhim_rank);

  return MDHIM_SUCCESS;
} 

/* ========== mdhimInsert ==========
   Insert a record into the data store

   fd is the MDHIM structure containing information on range servers and keys
   key_data_list is an array of keyDataList structures each with a primary key 
   value, any number of secondary keys and the record data
   num_key_data is the number of elements in the key_data_list array
   ierrors is the structure containing the highest insert error, number of operations that succeeded and an array of error codes for each insert. 

   Returns: MDHIM_SUCCESS on success or one of the following on failure 
   MDHIM_ERROR_INIT, MDHIM_ERROR_BASE or MDHIM_ERROR_MEMORY;
*/
int mdhimInsert(MDHIMFD_t *fd, struct keyDataList *key_data_list, int num_key_data, LISTRC *ierrors) {
  
  int err = MDHIM_SUCCESS;
  int k, j, i, start_range, server; 
  int *num_inserts = NULL, **insert_errors = NULL, **perrors = NULL;
  unsigned int len = 0;
  char **insert_data = NULL, **pdata = NULL;
  MPI_Request insert_request;
  MPI_Request *error_requests = NULL;
  
  PRINT_INSERT_DEBUG ("****************Rank %d Entered mdhimInsert****************\n", fd->mdhim_rank);
  /*
    Check input parameters
  */
  if(!fd){
    printf("Rank %d mdhimInsert: Error - MDHIM FD structure is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!key_data_list){
    printf("Rank %d mdhimInsert: Error - The array of key values and data is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!ierrors->errors){
    printf("Rank %d mdhimInsert: Error - The error array is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  
  /*
    Allocate memory for the array of pointers to the insert commands for each range server.
  */
  if( (insert_data = (char **)malloc(fd->rangeSvr_size * sizeof(char *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of insert commands.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  if( (pdata = (char **)malloc(fd->rangeSvr_size * sizeof(char *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of pointers to the insert commands.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  if( (num_inserts = (int *)malloc(fd->rangeSvr_size * sizeof(int))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of number of insert commands per server.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }

  ierrors->num_ops = 0;
  ierrors->max_return = 0;
  for(i = 0; i < fd->rangeSvr_size; i++){
    num_inserts[i] = 0;
    insert_data[i] = NULL;
  }
  /*
    For each record to insert, figure out what server and start range to send 
    to based on the primary (first) key. 
  */
  for(i = 0; i < num_key_data; i++){
    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Before whichStartRange with key = %s, key_type = %d, size = %d, max_recs = %d\n", fd->mdhim_rank, key_data_list[i].pkey, fd->pkey_type, key_data_list[i].pkey_length, fd->max_recs_per_range);
    
    err = getServerAndStartRange((void *)key_data_list[i].pkey, fd->pkey_type, fd->max_recs_per_range, fd->rangeSvr_size, &start_range, &server);
    
    ierrors->errors[i] = server;

    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: After whichStartRange with key = %s, server %d start_range %d\n", fd->mdhim_rank, key_data_list[i].pkey, server, start_range);
    /*
      If this is the first insert command for this server, allocate memory 
      for the insert command and initalize the string.
    */
    if(insert_data[server] == NULL) {
      len = 0;
      
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Insert data for server %d not allocated.\n", fd->mdhim_rank, server);
      
      if(fd->nkeys > 1){
	len = strlen(key_data_list[i].secondary_keys);
      }
      
      len += key_data_list[i].pkey_length + strlen(key_data_list[i].data) + 21;

      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Insert data %d for server %d has length %d.\n", fd->mdhim_rank, i, server, len);

      if( (insert_data[server] = (char *)malloc(num_key_data * len * sizeof(char))) == NULL){
	printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the insert commands for range server %d.\n", fd->mdhim_rank, server);
	return MDHIM_ERROR_MEMORY;
      }

      /*
	Compose the beginning of the insert command "insert"
      */
      memset(insert_data[server], '\0', num_key_data * len * sizeof(char));
      sprintf(insert_data[server], "insert %d ", num_key_data);
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Server %d insert command: %s with length %d.\n", fd->mdhim_rank, server, insert_data[server], (int)strlen(insert_data[server]));

      pdata[server] = &(insert_data[server][strlen(insert_data[server])]);

      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Server %d insert command: %s.\n", fd->mdhim_rank, server, insert_data[server]);
    }
    
    /*
      Compose the insert command; start range to insert key at, the 
      primary key, primary key length, secondary keys with secondary key 
      lengths, data length and data. Append successive records to insert.
    */
    num_inserts[server]++;
    
    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Server %d has %d inserts.\n", fd->mdhim_rank, server, num_inserts[server]);
    
    if(key_data_list[i].secondary_keys){
      sprintf(pdata[server], "%d %d %s %s %d %s", start_range, key_data_list[i].pkey_length, key_data_list[i].pkey, key_data_list[i].secondary_keys, (int)strlen(key_data_list[i].data), key_data_list[i].data);
    }
    else{
      sprintf(pdata[server], "%d %d %s %d %s", start_range, key_data_list[i].pkey_length, key_data_list[i].pkey, (int)strlen(key_data_list[i].data), key_data_list[i].data);
    }

    // shouldn't this work?
    //    pdata[server] += strlen(insert_data[server]);
    pdata[server] = &(insert_data[server][strlen(insert_data[server])]);

    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: data buffer for server %d is %s with size %u\n", fd->mdhim_rank, server, insert_data[server], (unsigned int)strlen(insert_data[server]));
    
  }

  /*
    Allocate memory for the array of return MDHIM errors and for the array 
    of MPI request structure for the MPI_Isend
  */
  if( (insert_errors = (int **)malloc(fd->rangeSvr_size * sizeof(int *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of insert errors for server %d.\n", fd->mdhim_rank, i);
    return MDHIM_ERROR_MEMORY;
  }
  
  if( (error_requests = (MPI_Request *)malloc(fd->rangeSvr_size * sizeof(MPI_Request))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of error MPI Request structures.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }

  /*
    For each range server, if there are records to insert, post receives 
    for error messages and send insert data.
  */
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(num_inserts[i] > 0){
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Before post of Ireceive for Insert error message from %d\n", fd->mdhim_rank, fd->range_srv_info[i].range_srv_num);
      
      if( (insert_errors[i] = (int *)malloc( (num_inserts[i]+1) * sizeof(int))) == NULL){
	printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of insert errors for server %d.\n", fd->mdhim_rank, i);
	return MDHIM_ERROR_MEMORY;
      }

      err = MPI_Irecv(insert_errors[i], num_inserts[i], MPI_INT, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &(error_requests[i]));
      
      if( err != MPI_SUCCESS){
	fprintf(stderr, "Rank %d mdhimInsert: ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
	return MDHIM_ERROR_COMM;
      }
      
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Posted Ireceive for Insert error message from %d\n", fd->mdhim_rank, fd->range_srv_info[server].range_srv_num);

      PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert sending message %s with size %d to server with rank %d\n", fd->mdhim_rank, insert_data[i], (int)strlen(insert_data[i]), fd->range_srv_info[i].range_srv_num); 
      
      err = MPI_Isend(insert_data[i], strlen(insert_data[i]), MPI_CHAR, fd->range_srv_info[i].range_srv_num, SRVTAG, fd->mdhim_comm, &insert_request);
      if( err != MPI_SUCCESS){
	fprintf(stderr, "Rank %d mdhimInsert: ERROR - MPI_Send of insert data for range server %d failed with error %d\n", fd->mdhim_rank, i, err);
	return MDHIM_ERROR_COMM;
      }
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Sent data to %d successful.\n", fd->mdhim_rank, fd->range_srv_info[i].range_srv_num);
    }
  }
    
    /*
      Now poll until ALL the non-blocking receives return.
    */
  //XXX This really should be a wait all and not a sequential wait for each request
  for(i = 0; i < fd->rangeSvr_size; i++)
    if(num_inserts[i] > 0)
      receiveReady(&(error_requests[i]), MPI_STATUS_IGNORE);
  
  for(i = 0; i < fd->rangeSvr_size; i++){
    for(j = 0; j < num_inserts[i]; j++){
      PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - server %d error %d = %d.\n", fd->mdhim_rank, i, j, insert_errors[i][j]);
    }
  }
    /*
      Now that all inserts are done, put the error codes in the correct place 
      in the output LISTRC struct
    */
  if( (perrors = (int **)malloc(fd->rangeSvr_size * sizeof(int *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of error pointers.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }

  for(i = 0; i < fd->rangeSvr_size; i++){
      perrors[i] = insert_errors[i];
  }

  k = 0;
  for(i = 0; i < fd->rangeSvr_size; i++){

    for(j = 0; j < num_inserts[i]; j++){
      if(num_inserts[i] > 0){
	ierrors->errors[k] = *perrors[ierrors->errors[k]]++;
	PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - server %d error %d = %d.\n", fd->mdhim_rank, i, j, ierrors->errors[k]);
	
	if(ierrors->errors[k] > ierrors->max_return){
	  ierrors->max_return = ierrors->errors[k];
	}
	if(ierrors->errors[k] == MDHIM_SUCCESS){
	  ierrors->num_ops++;
	}
	k++;
      }
    }
  }
  PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - %d successful inserts with max error %d.\n", fd->mdhim_rank,ierrors->num_ops, ierrors->max_return);
  PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - Inserting error code = %d.\n", fd->mdhim_rank, err);
  
  PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - fd->range_data.num_ranges = %d\n", fd->mdhim_rank, fd->range_data.num_ranges);
  
  for(i = 0; i < num_key_data; i++){
    PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - errors[%d] = %d\n", fd->mdhim_rank, i, ierrors->errors[i]);
  }
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(num_inserts[i] > 0){
      free(insert_errors[i]);
      free(insert_data[i]);
    }
  }
  free(error_requests);
  free(insert_data);
  free(pdata);
  free(perrors);
  free(insert_errors);
  
  PRINT_INSERT_DEBUG ("****************Rank %d Leaving mdhimInsert****************\n", fd->mdhim_rank);
  
  return err;
}

/* ========== mdhimOpen ==========
   Open key and data files on range servers
   
   mdhimOpen is a collective call, all processes participating in the job 
   must call this function.
   
   fd is the MDHIM structre that will be initalized with key information
   recordPath is the full path to the data store files
   mode is the mode to open the record files; 
   0 for create, 1 for update, 2 for read only 
   numKeys is the number of keys; primary key and any number of secondary keys
   keyType is an array specifying the key type for each of the numKeys, first 
   key is the primary key; 0 is alpha-numeric, 1 integer, 2 float
   keyMaxLen array of maximum length for each key of the numKeys
   keyMaxPad array of maximum padding for each key of the numKeys
   maxDataSize is the maximum size of the data in each record
   maxRecsPerRange is the size of each range; for integer keys the number 
   of records in the range, for float keys, this is ...

   Warning: The order of the pblIsamFile_t pointer array may not be the same 
   order of the final range_list because ranges are inserted into the 
   range_list and can move. The elements in the isam pointer array do not 
   track those moves. 

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimOpen(MDHIMFD_t *fd, char *recordPath, int mode, int numKeys, int *keyType, int *keyMaxLen, int *keyMaxPad, int maxDataSize, int maxRecsPerRange){

  char data[10];
  int err, open_error = MDHIM_SUCCESS;
  int i, j;
  MPI_Request open_request, error_request;

  PRINT_OPEN_DEBUG ("****************Rank %d Entered mdhimOpen****************\n", fd->mdhim_rank);
  /*
    Since open is a collective call, wait for all process to get here. 
  */
  PRINT_OPEN_DEBUG ("Rank %d: Inside MDHIM  Open before barrier\n", fd->mdhim_rank); 
  MPI_Barrier(fd->mdhim_comm);
  PRINT_OPEN_DEBUG ("Rank %d: Inside MDHIM Open after barrier\n", fd->mdhim_rank); 

  /*
    Check input parameters
  */
  if(!fd){
    printf("Rank %d: mdhimOpen Error - MDHIM FD structure is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!recordPath){
    printf("Rank %d: mdhimOpen Error - Path to store records is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!keyType){
    printf("Rank %d: mdhimOpen Error - Array of key types is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!keyMaxLen){
    printf("Rank %d: mdhimOpen Error - Array of maximum key lengths is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!keyMaxPad){
    printf("Rank %d: mdhimOpen Error - Array of maximum key padding is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(mode < 0 || mode > 2){
    printf("Rank %d: mdhimOpen Error - Invalid open mode (%d); 0 for open with create, 1 for update and 2 for read only.", fd->mdhim_rank, mode);
    return MDHIM_ERROR_INIT;
  }
  if(numKeys < 1){
    printf("Rank %d: mdhimOpen Error - Invalid total number of keys (%d); must be 1 or more.", fd->mdhim_rank, numKeys);
    return MDHIM_ERROR_INIT;
  }
  if(maxDataSize < 1){
    printf("Rank %d: mdhimOpen Error - Invalid maximum size of record data (%d); must be greater than 1.", fd->mdhim_rank, maxDataSize);
    return MDHIM_ERROR_INIT;
  }
  if( maxRecsPerRange < 1){
    printf("Rank %d: mdhimOpen Error - Invalid number of records per host (%d); must be 1 or more.", fd->mdhim_rank,  maxRecsPerRange);
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Fill in key information in the MDHIM fd structure and create the 
     structure for all alternate keys.
  */
  fd->max_recs_per_range = maxRecsPerRange;
  fd->pkey_type = keyType[0];
  fd->max_pkey_length = keyMaxLen[0];    
  fd->max_pkey_padding_length = keyMaxPad[0]; 
  fd->max_data_length = maxDataSize;    
  fd->nkeys = numKeys;
  fd->update = mode;

  if( (fd->alt_key_info = (struct altKeys *)malloc((numKeys - 1) * sizeof(struct altKeys)) ) == NULL){
    printf("Rank %d: mdhimOpen Error - Unable to allocate memory for the array of alternate key information.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  for(i=1; i < numKeys; i++){
    j = i - 1;
    fd->alt_key_info[j].type = keyType[i]; 
    fd->alt_key_info[j].max_key_length = keyMaxLen[i]; 
    fd->alt_key_info[j].max_pad_length = keyMaxPad[i]; 
  }
  
  //XXX For now assume no duplicate keys
  if( (fd->keydup = (int *)malloc(numKeys * sizeof(int)) ) == NULL){
    printf("Rank %d mdhimOpen: Error - Unable to allocate memory for the array of key duplicates.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  
  for(i=0; i < numKeys; i++)
    fd->keydup[i] = 0;
  
  /*
    For each range server, create the path to the data and key files.
  */
  PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Path to data files is %s\n", fd->mdhim_rank, recordPath);

  if( (fd->path = (char *)malloc(strlen(recordPath) + 20) ) == NULL){
    printf("Rank %d mdhimOpen: Error - Unable to allocate memory for the file path.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  memset(fd->path, '\0', strlen(recordPath) + 20);

  if(strncmp(&recordPath[strlen(recordPath) - 1], "/", 1) != 0)
    sprintf(fd->path, "%s/mdhimrank%d/", recordPath, fd->mdhim_rank);
  else
    sprintf(fd->path, "%smdhimrank%d/", recordPath, fd->mdhim_rank);
  
  PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: fd->path to data files is %s\n", fd->mdhim_rank, fd->path);

  /*
    Post a non-blocking received for the error codes from the open command 
    before sending data. This is just to help with deadlocking on send and 
    receives when you are sending to a range server thread that is your child. 
  */
  if( fd->range_srv_flag){

    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: I am a range server!\n", fd->mdhim_rank);

    // XX error when directory exists. Need to send back error no
    if(mkdir(fd->path,S_IRWXU) != 0){
      printf("Rank %d: mdhimOpen Error - Unable to create the directory %s.\n", fd->mdhim_rank, fd->path);
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Before error MPI_Irecv.\n", fd->mdhim_rank);
    err = MPI_Irecv(&open_error, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &error_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimOpen: ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }

    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: After error MPI_Irecv.\n", fd->mdhim_rank);
    /*
      Send the open command
    */
    memset(data, '\0', 10);
    strncpy(data, "open", 4);
    
    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Before MPI_Send of %s\n", fd->mdhim_rank, data);

    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->mdhim_rank, SRVTAG, fd->mdhim_comm, &open_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimOpen: ERROR - MPI_Send of open data failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Before receiveRequest.\n", fd->mdhim_rank);
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    if(open_error > 0){
      fprintf(stderr, "Rank %d mdhimOpen: ERROR -  Problem opening files with return error code %d.\n", fd->mdhim_rank, open_error);
      MPI_Abort(MPI_COMM_WORLD, 10);
    }
  }

  PRINT_OPEN_DEBUG ("Rank %d: Leaving MDHIM Open before barrier\n", fd->mdhim_rank); 
  MPI_Barrier(fd->mdhim_comm);
  PRINT_OPEN_DEBUG ("Rank %d: Leaving MDHIM Open after barrier\n", fd->mdhim_rank); 
  PRINT_OPEN_DEBUG ("****************Rank %d Leaving mdhimOpen****************\n", fd->mdhim_rank);
  
  return open_error;
}          

/* ========== receiveReady ==========
   Wait for and check if MPI_Irecv completed

   Input:
   inRequest is the MPI_Request struct
   inStatus is the MPI_Status struct

   Output:

   Returns: 0 on success
*/
int receiveReady( MPI_Request *inRequest, MPI_Status *inStatus) {
  
  int recv_ready = 0;
  int wait_count = 0;
  
  while( recv_ready == 0){
    MPI_Test(inRequest, &recv_ready, inStatus); // Should test for error
    wait_count++;
    
    if(wait_count%2 == 0){
      usleep(1000);
      //      printf("**mdhim_commands Waiting for recv_ready to be true. Wait count %d.\n", wait_count);
    }
    
  }
 
  return MDHIM_SUCCESS;
}

/* ========== setKeyDataList ==========
   Set the variables of the KeyDataList struct

   Input:

   Output:

   Returns: MDHIM_SUCCESS on success
*/
 int setKeyDataList( int my_rank, struct keyDataList *key_data_list, char *pkey, int pkey_len, char *data, int num_secondary_keys, char *skey_list) {
   
   int rc = MDHIM_SUCCESS, scanf_rc = 0;
   int i, data_len = 0, key_len = 0;
   char skeys[KEYSIZE];
   
   PRINT_MDHIM_DEBUG ("Rank %d Entered setKeyDataList with data = %s\n", my_rank, data);
   /*
     Check input parameters
   */
   if(key_data_list == NULL){
     printf("Rank %d setKeyDataList: Error - Input keyDataList struct is null.\n", my_rank);
     return(MDHIM_ERROR_INIT);
   }
   if(pkey == NULL){
     printf("Rank %d setKeyDataList: Error - Input primary key is null.\n", my_rank);
     return(MDHIM_ERROR_INIT);
   }
   if(data == NULL){
     printf("Rank %d setKeyDataList: Error - Input data is null.\n", my_rank);
     return MDHIM_ERROR_INIT;
   }
   if(num_secondary_keys > 0){
     if(key_data_list == NULL){
       printf("Rank %d setKeyDataList: Error - Input keyDataList structis null.\n", my_rank);
       return(MDHIM_ERROR_INIT);
     }
   }
   
   /*
     Set variables in the keyDataList structure
   */
   key_data_list->pkey_length = pkey_len;
   
   if( (key_data_list->pkey = (char *)malloc((pkey_len+1) * sizeof(char))) == NULL){
     printf("Rank %d setKeyDataList: Error - Problem allocating memory for the primary key.\n", my_rank);
     return MDHIM_ERROR_MEMORY;
   }
   memset(key_data_list->pkey, '\0', pkey_len+1);
   strncpy(key_data_list->pkey, pkey, pkey_len);
   
   data_len = strlen(data);
   if( (key_data_list->data = (char *)malloc((data_len+1) * sizeof(char))) == NULL){
     printf("Rank %d setKeyDataList: Error - Problem allocating memory for the records data.\n", my_rank);
     return MDHIM_ERROR_MEMORY;
   }
   memset(key_data_list->data, '\0', data_len+1);
   strncpy(key_data_list->data, data, data_len);
   
   /*
     Let's check and make sure we have all the secondary keys we think we 
     should. If so, copy them to the Key Data List.
   */
   if(num_secondary_keys > 0){
     if( (key_data_list->secondary_keys = (char *)malloc((strlen(skey_list)+1) * sizeof(char))) == NULL){
       printf("Rank %d setKeyDataList: Error - Problem allocating memory for the secondary keys.\n", my_rank);
       return MDHIM_ERROR_MEMORY;
     }
     memset(key_data_list->secondary_keys, '\0', strlen(skey_list)+1);
     strncpy(key_data_list->secondary_keys, skey_list, strlen(skey_list));
     
   } /* end if(num_secondary_keys) */
   else{
     key_data_list->secondary_keys = NULL;
   }
   
   return rc;
 }
