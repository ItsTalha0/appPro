#ifndef PROCESSOR_S_H
#define PROCESSOR_S_H

#include "partition.h"
#include "shared_memory.h"

int run_process();
void give_data_to_sender();
void send_msg_to_sender ();
void read_msg_from_sender();
void storelog(char * fmt, ...);
int retrive_comms_from_database(char *);
int store_comms_into_database(char *);
int retrive_data_from_database(char *); 
int prepare_statements();
void rollback(); 
int commit();
int connect_to_database();

typedef struct db_statements {
    char *statement_name;
    char *statement;
    int param_count;
}db_statements;


#define statement_count 9

db_statements dbs[statement_count] = {
    { 
      .statement_name = "s_get_info", 
      .statement = "SELECT sc.sfd, js.jobid FROM job_scheduler js JOIN sysinfo si ON js.jdestination = si.system_name \
                    JOIN sending_conns sc ON sc.sipaddr = si.ipaddress WHERE sc.scstatus = 2 AND js.jstate = 'S-4' \
                    ORDER BY js.jpriority::INT DESC LIMIT 1;",
      .param_count = 0,
    },
    { 
      .statement_name = "s_update_conns", 
      .statement = "UPDATE sending_conns SET scstatus = ($3), sfd = ($2)  WHERE sipaddr = ($1);",
      .param_count = 3,
    },
    { 
      .statement_name = "s_update_job_status2", 
      .statement = "UPDATE job_scheduler SET jstate = (SELECT CASE WHEN ($2) > 0 THEN 'C' ELSE 'D' END) WHERE jobid = $1::uuid;",
      .param_count = 2,
    },
    { 
      .statement_name = "s_get_comms", 
      .statement = "SELECT mtype, mdata1, mdata2, scommid FROM senders_comms WHERE mtype IN('1', '2') LIMIT 1;",
      .param_count = 0,
    },
    {
      .statement_name = "s_update_job_status1", 
      .statement = "UPDATE job_scheduler SET jstate = jstate || 'W' WHERE jobid = $1::uuid;",
      .param_count = 1, 
    },
    {
      .statement_name = "storelog", 
      .statement = "INSERT INTO logs (log) VALUES ($1);",
      .param_count = 1,
    },
    {
      .statement_name = "s_get_data",
      .statement = "SELECT jobdata FROM job_scheduler WHERE jobid = $1::uuid;",
      .param_count = 1,
    },
    {
      .statement_name = "s_update_comms",
      .statement = "UPDATE senders_comms SET mtype = mtype || 'W' WHERE scommid = $1::INTEGER;",
      .param_count = 1
    },
    {
      .statement_name = "s_delete_comms",
      .statement = "DELETE FROM senders_comms WHERE scommid = $1::INTEGER;",
      .param_count = 1
    }
};

#endif // PROCESSOR_H 

