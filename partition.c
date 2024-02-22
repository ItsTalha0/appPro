#include "partition.h"

// 1 = bitmap present at start representing TOTAL_PARTITIONS blocks 
// 2 = bitmap present at start representing TOTAL_PARTITIONS/2 blocks
// 3 = bitmap present at start but take next TOTAL_PARTITIONS/2 blocks
int get_subblock(char *block, int state, int type) 
{

    int i, j, begin, end;
    int bmap_size = TOTAL_PARTITIONS/8;
    char bmap[bmap_size];
    memcpy(&bmap, block, bmap_size); // retrive bitmap
    
    begin = 0;
    end = bmap_size;

    if (type == 1) 
        end = bmap_size/2;
    else if(type == 2)
        begin = bmap_size/2;

    for (i = begin; i<end; i++){
        for (j = 0; j<8; j++) {
            if (state == ((bmap[i]>>j) & 1) )
                return i*8+j;
        }
    }
    return -1;
}

// 1 = bitmap present at start representing TOTAL_PARTITIONS blocks 
// 2 = bitmap present at start representing TOTAL_PARTITIONS/2 blocks
// 3 = bitmap present at start but take next TOTAL_PARTITIONS/2 blocks
void toggle_bit(int idx, char *block) 
{    
    int bmap_size = TOTAL_PARTITIONS/8;
    char bmap[bmap_size];

    memcpy(&bmap, block, bmap_size); // retrive bitmap    
    bmap[idx/8] ^= 1<<(idx%8);  // toggle bit
    memcpy(block, &bmap, bmap_size);  // store bitmap


}


void set_all_bits(char *block, int type) 
{
    if (type == 1) 
        memset(block, 0xFFFFFFFF, 5);
    else if (type == 2)
        memset(block+5, 0xFFFFFFFF, 5);
    else if (type == 3)
        memset(block, 0xFFFFFFFF, 10);
}


void unset_all_bits(char *block, int type) 
{
    if (type == 1) 
        memset(block, 0, 5);
    else if (type == 2)
        memset(block+5, 0, 5);
    else if (type == 3)
        memset(block, 0, 10);
}