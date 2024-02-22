#include "user_program.h"

PGconn *connection;

int import_file (char *file_path) 
{
    char file_name[upbufsize];
    PGresult *res = NULL;
    
    int pathlen = strlen(file_path);
    file_path[pathlen-1] = 0;
    
    memset(file_name, 0, sizeof(upbufsize));
    memcpy(file_name, file_path, pathlen);

    const char *const param_values[] = {file_name, file_name};
    const int paramLengths[] = {pathlen, pathlen};
    const int paramFormats[] = {0, 0};
    int resultFormat = 0;

    res = PQexecPrepared(connection, dbs[0].statement_name ,dbs[0].param_count, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("failed to store file %s, error : %s\n", file_name, PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    printf("File %s succesfully stored in database\n", file_name);

    PQclear(res);
    return 0;
}


int send_file (char *file_name, char *destination)
{
    int len = 0;
    PGresult *res = NULL;

    len = strlen(file_name);
    if (file_name[len-1] == '\n') {
        file_name[len-1] = '\0';
    }

    len = strlen(destination);
    if (destination[len-1] == '\n') {
        destination[len-1] = '\0';
    }
    
    const char *const param_values[] = {file_name, destination};
    const int paramLengths[] = {strlen(file_name), strlen(destination)};
    const int paramFormats[] = {0, 0};
    int resultFormat = 0;

    // here param count is 1, so it is going to only use 1 parameter, rest will be ignored
    res = PQexecPrepared(connection, dbs[1].statement_name ,dbs[1].param_count, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("failed to get file info %s, error : %s\n", file_name, PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    if (PQntuples(res) < 1) {
        printf("file doesn't exists in database\n");
        PQclear(res);
        return -1;
    }
    
    PQclear(res);

    res = PQexecPrepared(connection, dbs[2].statement_name ,dbs[2].param_count, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("failed to create job for file sending %s, error : %s\n", file_name, PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    printf("Job succesfully created for %s\n", file_name);

    PQclear(res);
    return 0;
}


int export_file (char *path) 
{
    printf("not implemented %s\n", path);
    return 0;
} 


int show_stats() 
{
    int i, cnt;
    PGresult *res;
    char *line = "----------------------------------------------------------------------";


    res = PQexecPrepared(connection, dbs[3].statement_name ,dbs[3].param_count, NULL, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("failed to show stats error : %s\n", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }
    if ((cnt = PQntuples(res)) < 1) {
        printf("got no jobs\n");
        PQclear(res);
        return -1;
    }

    i = 0;

    printf("%s\n", line);
    printf(" %-10s | %-10s | %-10s | %-15s | %-10s |\n", "total_jobs", "jstate", "jpriority", "jdestination", "jtype");
    printf("%s\n", line);

    while (i < cnt) {
        printf(" %-10s | %-10s | %-10s | %-15s | %-10s |\n", PQgetvalue(res, i, 0), PQgetvalue(res, i, 1), PQgetvalue(res, i, 2), PQgetvalue(res, i, 3), PQgetvalue(res, i, 4));
        i+=1;
    }

    printf("%s\n", line);
    
    PQclear(res);
    return 0;
}


int show_jobs_info()
{
    int i, cnt;
    PGresult *res;
    char *line = "-------------------------------------------------------------------------------------------------------------------------------------------------------------";

    res = PQexecPrepared(connection, dbs[4].statement_name ,dbs[4].param_count, NULL, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("failed to show job info : %s\n", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }
    
    if ((cnt = PQntuples(res)) < 1) {
        printf("got no jobs\n");
        PQclear(res);
        return -1;
    }

    i = 0;

    printf("%s\n", line);
    printf(" %-37s | %-70s | %-10s | %-10s | %-15s |\n", "jobid", "file_name", "jstate", "jpriority", "jdestination");
    printf("%s\n", line);
    
    while (i < cnt) {
        printf(" %37s | %-70s | %-10s | %-10s | %-15s |\n", PQgetvalue(res, i, 0), PQgetvalue(res, i, 1), PQgetvalue(res, i, 2), PQgetvalue(res, i, 3), PQgetvalue(res, i, 4));
        i+=1;
    }
    
    printf("%s\n", line);

    PQclear(res);
    return 0;
}
                    

int update_job_priority(char *jobid, char *jobpriority)
{
    int len;
    PGresult *res;
    char pjobid[37];
    char pjobpriority[upbufsize];

    memset(pjobid, 0, sizeof(pjobid));
    memset(pjobpriority, 0, sizeof(pjobpriority));

    len = strlen(jobid);
    if (jobid[len-1] == '\n') {
        jobid[len-1] = '\0';
    }

    len = strlen(jobpriority);
    if (jobpriority[len-1] == '\n') {
        jobpriority[len-1] = '\0';
    }

    strncpy(pjobid, jobid, sizeof(pjobid));
    strncpy(pjobpriority, jobpriority, strlen(jobpriority));

    const char *const param_values[] = {pjobid, pjobpriority};
    const int paramLengths[] = {strlen(pjobid), strlen(pjobpriority)};
    const int paramFormats[] = {0, 0};
    int resultFormat = 0;

    res = PQexecPrepared(connection, dbs[5].statement_name ,dbs[5].param_count, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("failed to update job priority error : %s\n", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    printf("successfully update job priority\n");

    PQclear(res);
    return 0;
}


int show_files_info() 
{
    int i, cnt;
    PGresult *res;
    char *line = "---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------";

    res = PQexecPrepared(connection, dbs[6].statement_name ,dbs[6].param_count, NULL, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printf("failed to show files info : %s\n", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    if ((cnt = PQntuples(res)) < 1) {
        printf("got no jobs\n");
        PQclear(res);
        return -1;
    }

    i = 0;

    printf("%s\n", line);
    printf(" %-37s | %-70s | %-20s | %-50s |\n", "files_id", "file_name", "file_data_oid", "time_of_insertion");
    printf("%s\n", line);
    
    while (i < cnt) {
        printf(" %-37s | %-70s | %-20s | %-50s |\n", PQgetvalue(res, i, 0), PQgetvalue(res, i, 1), PQgetvalue(res, i, 2), PQgetvalue(res, i, 3));
        i+=1;
    }

    printf("%s\n", line);

    PQclear(res);
    return 0;
}


int connect_to_database (char *conninfo) 
{   
    connection = PQconnectdb(conninfo);

    if (PQstatus(connection) != CONNECTION_OK) {
        printf("Connection to database failed: %s\n", PQerrorMessage(connection));
        return -1;
    }

    return 0;
}


int prepare_statements () 
{    
    int i, status = 0;

    for (i = 0; i<statement_count; i++){

        PGresult* res = PQprepare(connection, dbs[i].statement_name, dbs[i].statement, dbs[i].param_count, NULL);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            printf("Preparation of statement failed: %s\n", PQerrorMessage(connection));
            status = -1;
            PQclear(res);
            break;
        }

        PQclear(res);
    }
    
    return status;
}


int get_input (char *prompt, char *buf, int bsize) 
{
    printf("%s", prompt);
    memset(buf, 0, bsize);

    if (fgets(buf, bsize, stdin) != NULL) {
        return 0;
    }

    printf("Invalid Input\n");
    return -1;    
}


int main (int argc, char *argv[]) 
{
    char choice1[3];
    char buf1[upbufsize];
    char buf2[upbufsize];    
    char db_conn_command[db_conn_command_size];
    char username[conn_param_size];
    char dbname[conn_param_size];
    int conffd = -1;

    if (argc != 2){
        printf("invalid arguments\n");
        return -1;
    }

    // open conf file
    if((conffd = open(argv[1], O_RDONLY)) == -1) {
        printf("failed to open config file\n");
        return -1;
    }

    memset(buf1, 0, upbufsize);
    // read conf file
    if(read(conffd, buf1, upbufsize) > 0){
        sscanf(buf1, "USERNAME=%s\nDBNAME=%s", username, dbname);
    }
    else {
        printf("failed to read config file");
        return -1;
    }

    close(conffd);

    // create command to connect to db
    sprintf(db_conn_command, "user=%s dbname=%s", username, dbname);

    if (connect_to_database(db_conn_command) == -1) { return -1;}
    if (prepare_statements() == -1) { return -1;}

    do {       
        printf("\nEnter 1 to import a file into database\n");
        printf("Enter 2 to send a file\n");
        printf("Enter 3 to export a file from database\n");
        printf("Enter 4 to show stats\n");
        printf("Enter 5 to show job info\n");
        printf("Enter 6 to Update job priority\n");
        printf("Enter 7 to show files present\n");
        printf("Enter 8 to close the program\n");
        fflush(stdin);
        memset(choice1, 0, sizeof(choice1));
        fgets(choice1, sizeof(choice1), stdin);    
    
        switch (choice1[0]) {
    
            case '8':   PQfinish(connection);
                        return 0;
                        break;
    
            case '1':   if (get_input("Enter absolute file path\n", buf1, upbufsize) == -1) {
                            break;
                        }
                        import_file(buf1);
                        break;
        
            case '2':   if (get_input("Enter file name\n", buf1, upbufsize) == -1) {
                            break;
                        }   
                        if (get_input("Enter destination\n", buf2   , upbufsize) == -1) {
                            break;
                        }
                        send_file (buf1, buf2);
                        break;
            
            case '3':   if (get_input("Enter absolute file path\n", buf1, upbufsize) == -1) {
                            break;
                        }
                        export_file(buf1);
                        break;
            
            case '4':   show_stats();
                        break;
            
            case '5':   show_jobs_info();
                        break; 
            
            case '6':   if (get_input("Enter jobid id\n", buf1, upbufsize) == -1) {
                            break;
                        }
                        if (get_input("Enter job priority\n", buf2, upbufsize) == -1) {
                            break;
                        }
                        update_job_priority(buf1, buf2);
                        break;

            case '7':   show_files_info();
                        break;

            default:    printf("Invalid Input\n");        
                        break;
        };

    } while(1);

}