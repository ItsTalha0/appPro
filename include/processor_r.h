#ifndef PROCESSOR_R_H
#define PROCESSOR_R_H

#include "partition.h"
#include "shared_memory.h"

void storelog(char * fmt, ...);
int run_process ();
int store_data_in_database (newmsg_data *);
int store_commr_into_database (receivers_message *);
void get_data_from_receiver ();
void get_message_from_receiver();
void send_msg_to_receiver ();
int get_comms_from_database (char *);
int connect_to_database ();
int prepare_statements ();

typedef struct db_statements {
    char *statement_name;
    char *statement;
    int param_count;
}db_statements;


#define statement_count 4

db_statements dbs[statement_count] = {
    { 
      .statement_name = "r_insert_data",  
      .statement = "INSERT INTO job_scheduler(jobdata, jstate, jtype, \
                    jsource, jobid, jparent_jobid, jdestination, \
                    jpriority) VALUES($2, 'N-1', '0', $1, GEN_RANDOM_UUID(), \
                    (select jobid from job_scheduler where jparent_jobid = jobid), 0, 5);",
      .param_count = 2,
    },
    { 
      .statement_name = "r_insert_conns", 
      .statement = "INSERT INTO receiving_conns (rfd, ripaddr, rcstatus) \
                    VALUES ($1, $2, $3) ON CONFLICT (rfd) DO UPDATE SET rcstatus = ($3);",
      .param_count = 3,
    },
    {
      .statement_name = "storelog", 
      .statement = "INSERT INTO logs (log) VALUES ($1);",
      .param_count = 1,
    },
    {
      .statement_name = "pr_getcomms", 
      .statement = "with rdata as (SELECT rcomid, rdata1, rdata2 FROM receivers_comms LIMIT 1) \
                  DELETE FROM receivers_comms WHERE rcomid = (SELECT rcomid FROM rdata) RETURNING rdata1, rdata2;",
      .param_count = 0,
    }
};

#endif // PROCESSOR_H 

