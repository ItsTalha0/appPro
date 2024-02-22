#include "shared_memory.h"
#include "partition.h"

// ftok present in sys/types is a function the provides a random key
// it accepts two arguments filename and a random integer
int get_key(char *filename, int project_id) 
{
    return ftok(filename, project_id);
}

int get_shared_block(char *filename, int size, char project_id) 
{
    // get the key
    key_t key = get_key(filename, project_id);
    if (key == -1) { return -1; }

    // get size of individual page of ram
    size_t page_size = sysconf(_SC_PAGESIZE);
    
    // round of the required block size to match the page size
    size_t rounded_size = ((size + page_size - 1) / page_size) * page_size;

    // get a shared memory segment id, if segment is already built using the key
    // then it will return the id of old segment
    return shmget(key, rounded_size, 0600 | IPC_CREAT);
}


char *attach_memory_block(char *filename, int size, char project_id) 
{   
    char *result;
    // get id of shared memory segment
    int shared_block_id = get_shared_block(filename, size, project_id);
    
    if (shared_block_id == -1) {
        return NULL;
    }

    // attach the shared memory segment to the address space of calling process
    result = (char *)shmat(shared_block_id, NULL, 0);
    if (result == (char *)-1) {
        return NULL;
    }

    return result;
}

// detahc memory block from address space of callin process
int detach_memory_block(char *block) 
{
    return (shmdt(block) != -1);
}


int destroy_memory_block(char *filename, char project_id)
{
    // get the id of shared memory block
    int shared_block_id = get_shared_block(filename, 0, project_id);

    if (shared_block_id == -1) {
        return -1;
    }

    // shared memory block is marked for destruction
    // the memory is actually freed when all processes detach themselves from this block
    return (shmctl(shared_block_id, IPC_RMID, NULL) != -1);
}
