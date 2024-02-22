#include <stdlib.h>
#include <syslog.h>
#include <string.h>
#include <errno.h>
#include "hashtable.h"



/* 
  create an array of pointers of type datanode
  as there is a chance of collison in hashtable, there will be chaining of node
 
  create a linkedlist(a nodepool) of nodes of type datanode 
  when a node is required, remove first element from nodepool and attach it to appropriate pointer in array
  if node/s are already present at the position, then attach the new node at the end of chain 
 
  to remove a node, get hash, find the node in chain, remove it, add it to the begining of the nodepool 
*/

// a somewhat good hash function
int hget_hash (hashtable *htable, char *key) {

    size_t i;
    int hash = hash_seed;
    
    for (i = 0; i < strlen(key); i++) {
        hash = ((hash << 5) + hash) + key[i];
    }

    return (hash % htable->table_size);
}


int hput (hashtable *htable, char *key, int value) {

    if (htable->available_node_count < 1) {
        return -1;
    }

    datanode **temp = NULL;

    int hash = hget_hash(htable, key);
    if (hash == -1) { return -2; }

    // use the hash to get the pointer for chain that contains the node
    temp = &htable->table[hash];

    // find empty location in chain, if chain is empty then its the first position
    while (*temp != NULL) {
        temp = &(*temp)->next;
    }

    // get first node from nodepool
    *temp = htable->nodepool;

    // adjust the nodepool
    htable->nodepool = htable->nodepool->next;
    
    // insert values in newnode
    strncpy((*temp)->key, key, strlen(key));
    (*temp)->value = value;
    (*temp)->next = NULL;

    // reduce free node count
    htable->available_node_count -= 1;

    return 0;
}


int hget (hashtable *htable, char *key) {

    datanode **temp = NULL;

    int hash = hget_hash(htable, key);
    if (hash == -1) { return -1; }

    // use the hash to get the pointer for chain that contains the node 
    temp = &htable->table[hash];

    // find the node in chain
    while (*temp != NULL && (strcmp((*temp)->key, key))) {
        temp = &(*temp)->next;
    }

    if (*temp == NULL) {
        return -2;
    }

    // return the value present in the node
    return (*temp)->value;
}


int hdel (hashtable *htable, char *key) {

    datanode **temp = NULL;
    datanode *freed_node = NULL;

    int hash = hget_hash(htable, key);
    if (hash == -1) { return -1; }

    // use the hash to get the pointer for chain that contains the node 
    temp = &htable->table[hash];

    // find the node in chain
    while (*temp != NULL && strcmp((*temp)->key, key)) {
        temp = &(*temp)->next;
    }

    if (*temp == NULL) {
        return -2;
    }
    
    // store the target node for temporary purpose
    freed_node = *temp;

    // remove the node
    *temp = (*temp)->next;
    
    // increase the count of free nodes
    htable->available_node_count += 1;
    
    //add the node to nodepool
    freed_node->next = htable->nodepool;
    htable->nodepool = freed_node;

    return 0;
}


int hcreate_table(hashtable *htable, int size) {

    size_t i = 0;
    datanode *temp;
    datanode *newnode;

    //get node count in hastable instance
    htable->available_node_count = htable->table_size = size;
    
    //allocate all memory for all the pointers of hashtable
    htable->table = (datanode **)calloc(sizeof(datanode *), htable->table_size);
    if (htable->table == NULL) { return -1; }

    temp = NULL;

    //allocate memory for all the nodes (count = size)
    for (i = 0; i<htable->table_size; i++) {
        
        newnode = (datanode *)calloc(sizeof(datanode), 1);
        if (newnode == NULL) { 
            htable = NULL;
            return -2; 
        }

        if (temp == NULL) {
            htable->nodepool = temp = newnode;
        }    
        else {
            temp->next = newnode;
            temp = temp->next;
        }
    }
   
    return 0;

}