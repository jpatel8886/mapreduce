#ifndef __mapreduce_h__
#define __mapreduce_h__

#include "safehash.h"
#include "safelist.h"

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what *you must implement*
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition);

///////////////////////////////////////////////////////////////////////////////

// typedef struct __Emit_item_t {
// 	char * key; 
// 	char * value; 
// } Emit_item_t; 	

// launches max_mappers number of threads at a time, to eventually map 
// all the files parallely into the hash table HT
int map_files(int argc, char *file_name[], Mapper mp, int max_mappers);
// sorts all the partitions of the hash table HT ascendingly  
void sort_partitions(SafeHash_t *h, int num_partitions);
// inserts into a partition such that it preserves the sorting 
void sorted_insert(SafeList_t *sorted_L, SafeNode_t *insert); 

///////////////////////////////////////////////////////////////////////////////

#endif // __mapreduce_h__