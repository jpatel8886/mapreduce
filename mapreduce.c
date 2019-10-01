#include "mapreduce.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

Partitioner pr; // Partitioner function
Mapper mp;      // Mapper function
Reducer rdr;    // Reducer function
int num_mprs;   // number of mappers
int num_rdrs;   // number of reducers
char ** files; 
int file_count, num_files; 

int *count, *max, *rd_id;     // global vars to keep track of where to insert in the 2D array
pthread_mutex_t lock;
pthread_mutex_t file_lock;         // only one thread can insert

#define INIT_COLS 100

typedef struct __Item_t
{
    char *key;
    char *value;
    int get_next_read;
} Item_t;

Item_t **MA; // global Main Array that holds partitions

//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

void init_map(int num, char ** argv)
{
    pthread_mutex_init(&file_lock, NULL);

    //Item_t * item; 

    files = (argv + 1); 
    file_count = 0;

    // initiate the count and the max column counts
    count = (int *)malloc(sizeof(int) * num);
    max = (int *)malloc(sizeof(int) * num);
    rd_id = (int *)malloc(sizeof(int) * num); 

    // num_partition number of Item_t pointers in the MA row
    MA = (Item_t **)malloc(sizeof(Item_t *) * num);

    // for each row
    for (int i = 0; i < num; i++)
    {
        // INIT_COLS number of Item_t
        MA[i] = (Item_t *)malloc(sizeof(Item_t) * INIT_COLS);
        // for (int j = 0; j < INIT_COLS; j++) {
        //     item = (Item_t*) malloc(sizeof(Item_t));
        //     MA[i][j] = *item; 
        // }

        max[i] = INIT_COLS - 1;
        count[i] = 0;
        rd_id[i] = i; 
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

void Insert (int partition_num, char *key, char *value)
{
    // column of this partition
    int col = count[partition_num];

    // allocate memory to this column of this partition
    // MA[partition_num][col] = *((Item_t *)malloc(sizeof(Item_t)));

    // create the item in this cell
    MA[partition_num][col].key = strdup(key);
    MA[partition_num][col].value = strdup(value);
    MA[partition_num][col].get_next_read = 0;

    // increment column count for this partition
    count[partition_num]++;

    // if count exceeds max columns for this partition
    if (count[partition_num] > max[partition_num])
    { 
        // increment max count by some amount
        max[partition_num] += INIT_COLS;
        
/*
        if (tmp != NULL)
            MA[partition_num] = tmp;
// */

        //void * clean = MA[partition_num]; 

        // allocate larger memory
        void *tmp = malloc(sizeof(Item_t)*max[partition_num]);
        assert(tmp != NULL);

        // copy old memory into this new memory
        memcpy(tmp, (void*)(MA[partition_num]), sizeof(Item_t) *(count[partition_num]));
        
        // pass the pointer of this old memory to the new one
        MA[partition_num] = (Item_t*)tmp; 

        // clean the old memory
        //free(clean);    

    }


if (partition_num == 0 && count[partition_num] == 34) {
    ;//printf("MA[%d][%d].key: %s\n", partition_num, col, MA[partition_num][col].key);
}


}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

void unused(Item_t *i)
{
    ;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

int check_read(char * key, int partition_number) {
    
    for (int i = 0; i < count[partition_number]; i++) {

        // if a similar key found
        if (strcmp(MA[partition_number][i].key, key) == 0) {
            if (MA[partition_number][i].get_next_read == 1) 
                return 1;
        }
    }

    return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

char *get_next(char *key, int partition_number)
{
    pthread_mutex_lock(&lock);

    // get the item from this partition, at the start
    
    for (int i = 0; i < count[partition_number]; i++)
    {
        if (strcmp(MA[partition_number][i].key, key) == 0)
        {

            // if this key hasnt been read by get_next
            if (MA[partition_number][i].get_next_read == 0)
            {
                MA[partition_number][i].get_next_read = 1;
                pthread_mutex_unlock(&lock);
                return MA[partition_number][i].value;
            }
        }
    }

    pthread_mutex_unlock(&lock);

    return NULL;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////
unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

void *MP(void *args)
{
    for (;;) 
    {
        char * file;
        
        pthread_mutex_lock(&file_lock);
        
        if (file_count >= num_files) {
            pthread_mutex_unlock(&file_lock);
            return NULL;
        }
    
        file = files[file_count++];
    
        pthread_mutex_unlock(&file_lock);
    
        mp(file);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////
void MR_Emit (char *key, char *value)
{
    // get the partition number where this key will be inserted
    int partition_number;

    pthread_mutex_lock(&lock);

    partition_number = pr(key, num_rdrs);

    Insert(partition_number, key, value);

    pthread_mutex_unlock(&lock);
}
/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////
void RD (void *partition_number)
{
    //printf("Inside RD\n");

    Getter getter = get_next;

    pthread_mutex_lock(&lock);
    int *pn = (int*)(partition_number);
    int p_num = rd_id[*pn];
    pthread_mutex_unlock(&lock);

    // sort partition MA[*pn] for count[*pn] items of size Item_t with Comparator defined comparisons
    //qsort(MA[*pn], count[*pn], sizeof(Item_t), Comparator); 

    for (int i = 0; i < count[p_num]; i++)
    {
        // TODO: How to handle multiple unique keys per list?
        pthread_mutex_lock(&lock);
        char *key = MA[p_num][i].key;
        pthread_mutex_unlock(&lock);

        if (check_read(key, p_num)) 
         continue;    

        rdr(key, getter, p_num);
    }    
}
/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

void print_map() {

    for (int i = 0; i < num_rdrs; i++) {
        for (int j = 0; j < count[i]; j++) {
            /* if (MA[i][j].key != NULL) */ //printf("MA[%d][%d].key : %s\n", i, j, MA[i][j].key); 
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

void free_mem() {

    for (int i = 0; i < num_rdrs; i++) {    
        for (int j = 0; j < INIT_COLS; j++) {
            free(&MA[i][j]); 
        }
            free (MA[i]);
    }
    free(count);
    free(max);
    free(MA);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

int Comparator(const void *a, const void *b)
{
    char *str1 = ((Item_t *)a)->key;
    char *str2 = ((Item_t *)b)->key;
    return strcmp(str1, str2);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{ 
    // initialize Main Array with num_reducers number of s ub arrays
    init_map (num_reducers, argv);

    // make MR_Run arguments available globally
    pr = partition;
    mp = map;
    rdr = reduce;
    num_mprs = num_mappers;
    num_rdrs = num_reducers;

    num_files = argc - 1; 

    pthread_t mapper_threads[num_mappers];

    for (int i = 0; i < num_mappers; i++)
    {
        int rc = pthread_create(&mapper_threads[i], NULL, MP, NULL);
        assert(rc == 0);
    }

    // wait until all created threads are joined
    for (int j = 0; j < num_mappers; j++)
    {
        int rc = pthread_join(mapper_threads[j], NULL);
        assert(rc == 0);
    }

    /*
    // sort all the rows
    for (int q = 0; q < num_reducers; q++) 
        qsort(MA[q], count[q]-1, sizeof(Item_t), Comparator);
    */

    // Launch the reducer threads
    pthread_t reducer_threads[num_reducers];

    // initiate threads to run on user defined Map()
    for (int k = 0; k < num_reducers; k++)
    {
        int rc = pthread_create(&reducer_threads[k], NULL, (void *)RD, (void *)&rd_id[k]);
        assert(rc == 0);
    }

    // wait until all created threads are joined
    for (int l = 0; l < num_reducers; l++)
    {
        int rc = pthread_join(reducer_threads[l], NULL);
        assert(rc == 0);
    }

// */

    // free_mem(); 

    //printf ("END\n");
}
/////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////
