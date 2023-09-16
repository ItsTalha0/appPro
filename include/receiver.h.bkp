#ifndef RECEIVER_H
#define RECEIVER_H

#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <stdint.h>
#include <netinet/in.h>
#include "partition.h"
#include "shared_memory.h"
#include "hashtable.h"

typedef struct server_info{
    int port;
    int maxevents;
    int servsoc_fd;
    int epoll_fd;
    unsigned long ipaddress;
}server_info;

typedef struct db_statements {
    char *statement_name;
    char *statement;
    int param_count;
}db_statements;


#define statement_count 1

db_statements dbs[statement_count] = {
    { 
      .statement_name = "storelog",  
      .statement = "INSERT INTO logs (log) VALUES ($1);",
      .param_count = 1
    }
};

int connect_to_database();
int prepare_statements();
void storelog(char * fmt, ...);
int run_receiver();
int accept_connection();
int remove_from_list(int);
int add_to_list(int);
int create_socket();
int make_nonblocking(int);
void send_to_processor(newmsg_data *);
void send_message_to_processor(receivers_message *);
int get_message_from_processor (capacity_info *cpif);
int end_connection (int fd, struct sockaddr_in addr);

#endif //RCEIVER_H
