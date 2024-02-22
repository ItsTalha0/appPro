#ifndef SENDER_H
#define SENDER_H

#include <sys/socket.h> // contains important fucntionality and api used to create sockets   
#include <netinet/in.h> // contains structures to store address information
#include <sys/time.h>
#include "partition.h"
#include "shared_memory.h"


int create_connection(unsigned short int, int); 
int get_message_from_processor(char *); 
int get_data_from_processor(send_message *);
void send_message_to_processor(int, void *); 
int run_sender(); 
void storelog(char * fmt, ...);

typedef struct db_statements {
    char *statement_name;
    char *statement;
    int param_count;
}db_statements;

int connect_to_database();
int prepare_statements();

#define statement_count 1

db_statements dbs[statement_count] = {
    { 
      .statement_name = "storelog",  
      .statement = "INSERT INTO logs (log) VALUES ($1)",
      .param_count = 1
    }
};

#endif // SENDER_H
