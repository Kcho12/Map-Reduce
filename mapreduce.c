#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

#define NUM_MAPS 1000
pthread_mutex_t fileLock;
int fileCount; 
int num_files;
int num_reducer;
char** file_names;

//global partioner, mapper, and reducer values obtained from args in MR_Run
Partitioner g_partitioner;
Mapper g_mapper;
Reducer g_reducer;

typedef struct value_node{
    char* value;
    struct value_node* next;
} value_node;
typedef struct key_node{
    char* key;
    value_node* head;
    struct key_node* next;
} key_node;
typedef struct key_entry {
    key_node* head;
    pthread_mutex_t lock;
} key_entry;
typedef struct partition_entry{
    key_entry map[NUM_MAPS];
    int key_num; //keeps track of number of keys in the main file
    key_node* node_entry;
    int curr_position;
    pthread_mutex_t lock;
} partition_entry;

partition_entry hash[64];

void* threadMapper(void* arg){
    for (;;){
        char* fileName;
        pthread_mutex_lock(&fileLock);
        if(fileCount >= num_files){
            pthread_mutex_unlock(&fileLock);
            return NULL;
        }
        fileName = file_names[fileCount++];
        pthread_mutex_unlock(&fileLock);
        g_mapper(fileName);
    }
}

int compareKey(const void* key1, const void* key2){
    char* a = ((key_node*)key1)->key;
    char* b = ((key_node*)key2)->key;
    return strcmp(a, b);
}

char* get_next(char *key, int partition_number){
    key_node* k_node = hash[partition_number].node_entry;
    char* value;
    while(1){
        int position = hash[partition_number].curr_position;
        if (strcmp(k_node[position].key, key) == 0){//checks for match if keys are the same then count++ occurs in the reducer function
            if (k_node[position].head == NULL)
                return NULL;
            value_node* temp = k_node[position].head->next;
            value = k_node[position].head->value;
            k_node[position].head = temp;
            return value;
        } else {//if no match go to next position
            hash[partition_number].curr_position++;
            continue;
        }
        return NULL;
    }
}

void* threadReducer(void* arg){
    int partition_number = *(int*)arg;
    free(arg); 
    arg = NULL;
    if(hash[partition_number].key_num == 0){
        return NULL;
    }
    hash[partition_number].node_entry = malloc(sizeof(key_node)*hash[partition_number].key_num);
    int count = 0;
    for (int i = 0; i < NUM_MAPS; i++){
        key_node *position = hash[partition_number].map[i].head;
        if (position == NULL)
            continue;
        while (position != NULL){
            hash[partition_number].node_entry[count] = *position;
            count++;
            position = position -> next;
        }
    }
    qsort(hash[partition_number].node_entry, hash[partition_number].key_num, sizeof(key_node), compareKey);
   
    for (int i = 0; i < hash[partition_number].key_num; i++){
        char *key = hash[partition_number].node_entry[i].key;
        g_reducer(key,get_next,partition_number);
    }

    //Memory Cleanup
    for (int i = 0; i < NUM_MAPS; i++){
        key_node *position = hash[partition_number].map[i].head;
        if (position == NULL)
            continue;
        while (position != NULL){
            free(position->key);
            position->key = NULL;
            value_node* vcurr = position->head;
            while (vcurr != NULL){
                free(vcurr->value);
                vcurr->value = NULL;
                value_node* temp = vcurr -> next;
                free(vcurr);
                vcurr = temp;
            }
            vcurr = NULL;
            key_node* tempK = position -> next;
            free(position);
            position = tempK;
        }
        position = NULL;
    }
    free(hash[partition_number].node_entry);
    hash[partition_number].node_entry = NULL;
    return NULL;
}

void MR_Emit(char * key, char * value)
{
    unsigned long partition_number = g_partitioner(key, num_reducer);
    unsigned long map_number = MR_DefaultHashPartition(key, NUM_MAPS);
    pthread_mutex_lock(&hash[partition_number].map[map_number].lock);
    key_node* temp = hash[partition_number].map[map_number].head;
    while(temp != NULL){
        if (strcmp(temp->key, key) == 0){
            break;
            }
        temp = temp->next;
    }

    value_node* v_node = malloc(sizeof(value_node));
    if (v_node == NULL) {//failure
        perror("malloc");
        pthread_mutex_unlock(&hash[partition_number].map[map_number].lock);
        return; 
    } 
    v_node->value = malloc(sizeof(char)*20);
    strcpy(v_node->value, value);
    v_node->next = NULL;
    //malloc new node if we find new key
    if (temp == NULL){
        key_node *k_node = malloc(sizeof(key_node));
        if (k_node == NULL) {
            perror("malloc");
            pthread_mutex_unlock(&hash[partition_number].map[map_number].lock);
            return; 
        }
        k_node->head = v_node;
        k_node->next = hash[partition_number].map[map_number].head;
        hash[partition_number].map[map_number].head = k_node;
        
        k_node->key = malloc(sizeof(char)*20);
        strcpy(k_node->key, key);
        pthread_mutex_lock(&hash[partition_number].lock);
        hash[partition_number].key_num++;//keeps track of the number of that specific key
        pthread_mutex_unlock(&hash[partition_number].lock);

    } else {
        //if key already exists then set v_node to next
        v_node->next = temp->head;
        temp->head = v_node;
    }

    pthread_mutex_unlock(&hash[partition_number].map[map_number].lock);
}

unsigned long MR_DefaultHashPartition (char * key, int num_buckets)
{
    //default function from README
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_buckets;
}

void MR_Run (int argc, char * argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{
    int rc = pthread_mutex_init(&fileLock, NULL);
    assert(rc == 0);
    fileCount = 0;
    num_reducer = num_reducers;
    num_files = argc - 1;

    file_names = (argv + 1);
    g_partitioner = partition;
    g_mapper = map;
    g_reducer = reduce;

    //hash init
    for (int i = 0;i < num_reducer; i++){
        pthread_mutex_init(&hash[i].lock, NULL);
        hash[i].key_num = 0;
        hash[i].curr_position = 0;
        hash[i].node_entry = NULL;
        for (int j = 0; j < NUM_MAPS; j++){
            hash[i].map[j].head = NULL;
            pthread_mutex_init(&hash[i].map[j].lock, NULL);
        } 
    }

    //thread creation. First mappers run then join. Second reducers run then join.
    pthread_t mapperThreads[num_mappers];
    pthread_t reducerThreads[num_reducers];

    for (int i = 0; i < num_mappers; i++){
        pthread_create(&mapperThreads[i], NULL, threadMapper, NULL);
    }

    for (int j = 0; j < num_mappers; j++){
        pthread_join(mapperThreads[j], NULL);
    }

    for (int k = 0; k < num_reducers; k++){
        void* arg = malloc(4);
        *(int*)arg = k;
        pthread_create(&reducerThreads[k], NULL, threadReducer, arg);
    }

    for (int l = 0; l < num_reducers; l++){
        pthread_join(reducerThreads[l], NULL);
    }
}

