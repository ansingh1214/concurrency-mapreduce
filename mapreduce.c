#include "mapreduce.h"
#include <stdbool.h>
#include <pthread.h>
//#include <sys/sysinfo.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#define SIZE 100000000
#define STR_SIZE 100
#define NUM_LOCKS 60
//each partition has its each lock for a better time complexity
pthread_mutex_t locks[NUM_LOCKS];
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

//globals for use
int reducers;
Partitioner global_part;
Reducer global_reduce;
Mapper global_map;
long total_file_size;
int args;
char** filename = NULL;
typedef struct map_data
{
    Mapper map;
}map_data;
//struct for Reducer helper
typedef struct reduce_data
{
    int part_num;
}reduce_data;

//index of each partition
typedef struct hashindex
{
    char * keys;
    char * val;
}hashindex;
int count;
//declaring the partition
hashindex*** partitions;
//stores the size of each partition
int* indices;
//global used for get_next
int* curr;
//index of second key of each partition
int* second_index;
void MR_Emit(char *key, char *value)
{
          int part_num = global_part(key, reducers);
          int index;          
          //lock for the current partition
          pthread_mutex_lock(&locks[part_num]);
          index = indices[part_num];
          indices[part_num]++;
          pthread_mutex_unlock(&locks[part_num]);
          //unlock for the current partition          
          //inputs the keys and values into the partition
          hashindex *tmp = (hashindex*) calloc(1, sizeof(hashindex*));
          tmp->keys = malloc(STR_SIZE);
          tmp->val =  malloc(STR_SIZE);
          strcpy(tmp->keys, key);
          strcpy(tmp->val, value);
          partitions[part_num][index] = (hashindex*) calloc(1, sizeof(hashindex*));
          partitions[part_num][index] = tmp;
          tmp = NULL;
          count++;
}

// helper for Mapper
int file_index = 0;
void * map_helper(void * data)
{
    map_data * mdata = (map_data*) data;
    int index = 0;
    while(file_index < args)
    {
        pthread_mutex_lock(&lock);
        index = file_index;
        file_index++;
        pthread_mutex_unlock(&lock);
        if(filename[index] == NULL)
        {
            return NULL;
        }
        char* file = malloc(STR_SIZE);
        strcpy(file, filename[index]);
        mdata->map(file);
    }
    return NULL;
}

//helper for sorting
int compare_struct(const void* a, const void* b)
{
           hashindex* x = *(hashindex**)a;
           hashindex* y = *(hashindex**)b;
           return strcmp(x->keys, y->keys);
}

//get_next function
char* get_next(char* key, int partition_number)
{
        if(curr[partition_number] >= indices[partition_number])
        {
            return NULL;
        }
        if(strcmp(partitions[partition_number][curr[partition_number]]->keys, key) == 0)
        {
            int x = curr[partition_number];
            curr[partition_number]++;
            return partitions[partition_number][x]->val;
        }
        return NULL;
}
char* get_first(char* key,  int partition_number)
{
    if(curr[partition_number] >= indices[partition_number])
    {
        return NULL;
    }
    if(curr[partition_number] < second_index[partition_number])
    {
        int x = curr[partition_number];
        curr[partition_number]++;
        return partitions[partition_number][x]->val;
    }
    return NULL;
}

//helper for reducer
void * reduce_helper(void * data)
{
    reduce_data * rdata = (reduce_data*) data;
    int partition_number = rdata->part_num;
    //printf("REDUCE %d STARTED\n", partition_number); 
    if(indices[partition_number] == 0 || partitions[partition_number] == NULL || partitions[partition_number][0] == NULL)
    {
        return NULL;
    }
    qsort((void*)&partitions[partition_number][0], indices[partition_number], sizeof(hashindex*), compare_struct);
    //printf("PARTITION %d SORTED\n", partition_number);
    char* prev_key = malloc(STR_SIZE);
    strcpy(prev_key, partitions[partition_number][0]->keys);
    int second = 1;
    //while((strcmp(partitions[partition_number][second]->keys, prev_key) == 0) && (second < indices[partition_number]))
    while(1)
    {
        if(partitions[partition_number][second] == NULL)
        {
            break;
        }
        if(prev_key == NULL)
        {
            break;
        }
        if(partitions[partition_number][second]->keys == NULL)
        {
            break;
        }
        if(second >= indices[partition_number])
        {
            break;
        }
        if(strcmp(partitions[partition_number][second]->keys, prev_key) == 0)
        {
            second ++;
        }
        else
        {
            break;
        }
    }
    second_index[partition_number] = second;
    global_reduce(prev_key, get_first, partition_number);
    for(int i = second; i < indices[partition_number]; i++)
    {
        if(strcmp(partitions[partition_number][i]->keys, prev_key) != 0)
        {
            free(prev_key);
            prev_key = malloc(STR_SIZE);
            strcpy(prev_key, partitions[partition_number][i]->keys);
            global_reduce(prev_key, get_next, partition_number);
        }
        free(partitions[partition_number][i]->keys);
        free(partitions[partition_number][i]->val);
        free(partitions[partition_number][i]);
    }
    free(partitions[partition_number]);
    //printf("REDUCE %d DONE\n", partition_number);
    return NULL;
}


//default partition function
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

//MR_RUN implementation!
void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition)
      {
      file_index = 0;
      count = 0;
      args = argc - 1;
      global_map = map;
      global_reduce = reduce;
      reducers = num_reducers;
      global_part = partition;
      curr = malloc(num_reducers * sizeof(int));
      second_index = malloc(num_reducers * sizeof(int));
      indices = malloc(num_reducers * sizeof(int));
      partitions = calloc (num_reducers, sizeof(hashindex**));
      for(int i = 0; i < num_reducers; i++)
      {
          partitions[i] = calloc(SIZE, sizeof(hashindex*));
      }
      for(int i = 0; i < num_reducers; i++)
      {
          pthread_mutex_init(&locks[i], NULL);
      }
      for(int i = 0 ; i < num_reducers; i++)
      {
          indices[i] = 0;
          curr[i] = 0;
          second_index[i] = 1;
      }
      //storing the files
      FILE *fp [args];
      //data->filenames = (char**) malloc(sizeof(char*)*args);
      filename =(char**) malloc(sizeof(char*) * args);
      for(int i = 0;i<args;i++)
      {
           filename[i] =(char*) malloc(STR_SIZE);
      }
      for(int i = 1; i < argc; i++)
      {
            strcpy(filename[i - 1], argv[i]);
      }
      //long *file_size = malloc(sizeof(int) * args);
      long *file_size = (long*) malloc(sizeof(long) * args);
      //computing the filesize
      for(int i = 0; i < args; i++)
      {
          fp[i] = fopen(argv[i + 1], "r");
          fseek(fp[i], 0L, SEEK_END);
          file_size[i] = ftell(fp[i]);
          fseek(fp[i], 0L, SEEK_SET);
          total_file_size += file_size[i];
      }
      //using bubble sort sort files
      for(int i = 0; i < args; i++)
      {
          for(int j = 0; j < args - i -1; j++)
          {
              if(file_size[j] > file_size[j+1])
              {
                  
                  long temp = file_size[j];
                  file_size[j] = file_size[j+1];
                  file_size[j+1] = temp;
                  
                  char*  tmp = malloc(100);
                  strcpy(tmp, filename[j]);
                  strcpy(filename[j], filename[j+1]);
                  strcpy(filename[j+1], tmp);
              }
          }
      }
      free(file_size);
      //initialize and create threads for mapping
      map_data mdata;
      mdata.map = map;
      pthread_t mappers[num_mappers];
      pthread_t threads;
      int save;
      for (int i = 0; i < num_mappers; i++)
      {
          save = pthread_create(&threads, NULL, map_helper, &mdata);
          if (save == 0)
          {
              mappers[i] = threads;
          }
      }
      //join mapper threads
      for (int i = 0; i < num_mappers; i++)
      {
          pthread_join(mappers[i],NULL);
      }

      /*for(int i = 0; i < num_reducers; i++)
      {
          for(int j = 0; j < indices[i]; j++)
          {
                char *tmp = malloc(100);
                printf("%d   :    %d\n", i,j);
                strcpy(partitions[i][j]->keys, tmp);
          }
      }*/
      //initialize structs for reducing
      reduce_data rdata[num_reducers];
      for(int i = 0; i < num_reducers; i++)
      {
          rdata[i].part_num = i;
      }
      //initialize and create threads for reducing
      pthread_t reducers[num_reducers];
      for(int i = 0; i < num_reducers; i++)
      {
          if(indices[i] != 0)
          {
                save = pthread_create(&threads, NULL, reduce_helper, &rdata[i]);
                if(save == 0)
                {
                    reducers[i] = threads;
                }
          }
      }
      //join reducers threads
      for(int i = 0 ; i < num_reducers; i++)
      {
          if(indices[i] != 0)
          {
                pthread_join(reducers[i], NULL);
          }
      }
}
