#ifndef _USR_PRGM_H
#define _USR_PRGM_H

#include "partition.h"

#define upbufsize 260
#define db_conn_command_size 100
#define conn_param_size 30

typedef struct db_statements {
    char *statement_name;
    char *statement;
    int param_count;
}db_statements;


#define statement_count 7

db_statements dbs[statement_count] = {
    {
      .statement_name = "import_file",
      .statement = "INSERT INTO files(file_id, file_name, file_data) VALUES(GEN_RANDOM_UUID(), $1, lo_import($2));",
      .param_count = 2,
    },
    {
      .statement_name = "check_file_existence",
      .statement = "SELECT 1 FROM files WHERE file_name = $1;",
      .param_count = 1,
    },
    {
      .statement_name = "create_job_for_sending",
      .statement = "INSERT INTO job_scheduler(jobdata, jstate, jtype, jsource, jobid, jparent_jobid, jdestination, jpriority) VALUES($1, 'S', lpad('6', 5, ' '), (select jsource from job_scheduler where jparent_jobid = jobid), GEN_RANDOM_UUID(), (select jobid from job_scheduler where jparent_jobid = jobid), lpad($2, 5, ' '), 5);",
      .param_count = 2,
    },
    {
      .statement_name = "show_stats",
      .statement = "SELECT * FROM show_stats;",
      .param_count = 0,
    },
    {
      .statement_name = "show_jobs_info",
      .statement = "SELECT * FROM show_jobs_info;",
      .param_count = 0,
    },
    {
      .statement_name = "update_job_priority",
      .statement = "UPDATE job_scheduler SET jpriority = $2 WHERE jparent_jobid = $1::uuid OR jobid = $1::uuid;",
      .param_count = 2,
    }, 
    {
      .statement_name = "get_files_info",
      .statement = "SELECT * FROM show_files_info;",
      .param_count = 0,
    }
};


int prepare_statements ();
int connect_to_database (char *); 
int import_file (char *);
int export_file (char *);
int send_file (char *, char *);
int get_input (char *, char *, int);

#endif //_USR_PRGM_H
