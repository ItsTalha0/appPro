#include "processor_s.h"

semlocks sem_lock_datas;
semlocks sem_lock_comms;
semlocks sem_lock_sigs;
semlocks sem_lock_sigps;
datablocks datas_block;
datablocks comms_block;
PGconn *connection;

const struct timespec tm = {
    .tv_sec = 1,
    .tv_nsec = 0L,
};


void rollback() 
{
    PGresult *res = PQexec(connection, "ROLLBACK");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s%s", "ROLLBACK command for transaction failed in processor_s : ", PQerrorMessage(connection));
    }
    PQclear(res);
}


int commit() 
{
    int status = 0;

    PGresult *res = PQexec(connection, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s%s", "COMMIT for transaction command failed in processor_s : ", PQerrorMessage(connection));  
        rollback();
        status = -1;
    }    

    PQclear(res);
    return status;
}


int retrive_data_from_database (char *blkptr) 
{
    PGresult *res;
    send_message *sndmsg = (send_message *)blkptr;
    
    // begin the transaction
    res = PQexec(connection, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s%s","BEGIN command to start transaction failed in processor_s : ", PQerrorMessage(connection));
        PQclear(res); 
        return -1;
    }
    else {
    
        PQclear(res);
        // retrives file descriptor and message uuid from database
        res = PQexecPrepared(connection, dbs[0].statement_name, dbs[0].param_count, NULL, NULL, NULL, 0);

        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            storelog("%s%s", "failed to retrive info from db : ", PQerrorMessage(connection));
            rollback();
            PQclear(res);
            return -1;
        }    
        else if (PQntuples(res) > 0) {
            
            // store retrived data in appropriate data structure
            sndmsg->fd = atoi(PQgetvalue(res, 0, 0));
            memcpy(sndmsg->uuid, PQgetvalue(res, 0, 1), PQgetlength(res, 0, 1));
            sndmsg->uuid[36] = '\0';

            const char *const param_values[] = {sndmsg->uuid};
            const int paramLengths[] = {sizeof(sndmsg->uuid)};
            const int paramFormats[] = {0};
            int resultFormat = 1; 
            // for data of type byte array, result format must be 1 that is binary
            // in other casses it needs to be 0, 0 represents the character formate
            // hence, first uuid and fd are retrived with result format set to 0
            // then based on uuid data is retrived with result format set to 1
            
            // based on uuid retrive datat
            PQclear(res);
            res = PQexecPrepared(connection, dbs[6].statement_name, dbs[6].param_count, param_values,   
                                            paramLengths, paramFormats, resultFormat);
            
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                storelog("%s%s", "failed to retrive data from db  : ", PQerrorMessage(connection));
                rollback();
                PQclear(res);
                return -1;
            }
            else if (PQntuples(res) > 0) {
                
                // if data is successfully retrived, update status of message from read state to waiting state
                sndmsg->size = PQgetlength(res, 0, 0);
                memcpy(sndmsg->data, PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
               
                PQclear(res);
                res = PQexecPrepared(connection, dbs[4].statement_name, dbs[4].param_count, 
                                        param_values, paramLengths, paramFormats, resultFormat);

                if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                    storelog("%s%s", "failed update job status : ", PQerrorMessage(connection));
                    rollback();
                    PQclear(res);
                    return -1;
                }
                else {
                    PQclear(res);
                    return commit();
                }
            }
            else {
                storelog("%s"," processor s got no data rollback");
                rollback();
                PQclear(res);
                return -1;
            }
        }
        else {
            storelog("%s", " processor s got no tuples info rollback");
            rollback();
            PQclear(res);
            return -1;
        }
    }

    return -1;  
}

// stores communication received from sender into database
int store_comms_into_database (char *blkptr) 
{
    int resultFormat = 0;
    char ipaddress[11];
    char fd[11];
    char mstatus[11];
    char id[37];

    PGresult* res;
    memset(id, 0, sizeof(id));

    // if message is of type 3, then it represents a message holding information about connection status
    if(*(int *)blkptr == 3) {

        connection_status *cncsts = (connection_status *)blkptr;
        sprintf(ipaddress, "%d", cncsts->ipaddress);        
        sprintf(fd, "%d", cncsts->fd);

        if (cncsts->fd >= 0) {
            sprintf(mstatus, "%d", 2);
        }
        else{
            sprintf(mstatus, "%d", 1);
        }
        
        const char *param_values[] = {ipaddress, fd, mstatus};
        const int paramLengths[] = {sizeof(ipaddress), sizeof(fd), sizeof(mstatus)};
        const int paramFormats[] = {0, 0, 0};
        
        res = PQexecPrepared(connection, dbs[1].statement_name, dbs[1].param_count, param_values, paramLengths, paramFormats, resultFormat);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            storelog("%s%s", "failed to update connection status : ", PQerrorMessage(connection));
            PQclear(res);
            return -1;
        }
        else {

            sprintf(id, "%d", cncsts->scommid); 
            
            const char *param_values[] = {id};
            const int paramLengths[] = {sizeof(id)};
            const int paramFormats[] = {0};
            
            // the message that was created by db to modify connection
            // has been read, sender sent the status of modification
            // the status has been updated in database
            // since the connection status is completely updated
            // the message that was created by db, to modify the connection has served its purpose
            // now delete this message

            PQclear(res);
            res = PQexecPrepared(connection, dbs[8].statement_name, dbs[8].param_count, param_values, paramLengths, paramFormats, resultFormat);
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                storelog("%s%d%s%s", "failed to delete comms id : ",  cncsts->scommid,  " errro : ", PQerrorMessage(connection));
                PQclear(res);
                return -1;
            }

        }
    }
    else if(*(int *)blkptr == 4) {
        
        // this type of message indicates, stauts of message that was suppose to be sent

        message_status *msgsts = (message_status *)blkptr;
        
        sprintf(mstatus, "%hhu", msgsts->status);
        memcpy(id, msgsts->uuid, sizeof(msgsts->uuid));
        id[36] = '\0';
        
        const char *param_values[] = {id, mstatus};
        const int paramLengths[] = {sizeof(id), sizeof(mstatus)};
        const int paramFormats[] = {0, 0};
        
        res = PQexecPrepared(connection, dbs[2].statement_name, dbs[2].param_count, param_values, paramLengths, paramFormats, resultFormat);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            storelog("%s%s%s%s%s%s%s", "original", msgsts->uuid,"failed to update message status uuid : " , id ," mstatus : ", mstatus ,PQerrorMessage(connection));
            PQclear(res);
            return -1;   
        }
    }
    else {
        storelog("%s", "invalid type of message received in processors store comms into database");
        return -1;
    }

    PQclear(res);
    return 0;
}


// retirives comms for sender from database
int retrive_comms_from_database (char *blkptr) 
{
    PGresult *res;
    int type;
    char scommid[30];

    // read the message from database
    res = PQexecPrepared(connection, dbs[3].statement_name, dbs[3].param_count, NULL, NULL, NULL, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        storelog("%s%s", "retriving comms for sender failed : ", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }    
    if (PQntuples(res) > 0) {
       
        type = atoi(PQgetvalue(res, 0, 0));

        if (type == 1) {
            
            // this type of message for opening a new connection

            open_connection *opncn = (open_connection *)blkptr;
            opncn->type = atoi(PQgetvalue(res, 0, 0));
            opncn->ipaddress = atoi(PQgetvalue(res, 0, 1)); 
            opncn->port = atoi(PQgetvalue(res, 0, 2));
            opncn->scommid = atoi(PQgetvalue(res, 0, 3));

            memcpy(scommid, PQgetvalue(res, 0, 3), PQgetlength(res, 0, 3));
            const char *param_values[] = {scommid};
            const int paramLengths[] = {sizeof(scommid)};
            const int paramFormats[] = {0};
            int resutlFormat = 0;

            // if message is read succesfully then update its status to waiting state 
            PQclear(res);
            res = PQexecPrepared(connection, dbs[7].statement_name, dbs[7].param_count, param_values, paramLengths, paramFormats, resutlFormat);
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                storelog("%s%s", "failed to update comms status : ", PQerrorMessage(connection));
                PQclear(res);
                return -1;
            }
        }
        else if(type == 2) {
            
            // this type of message if for closing a connection

            close_connection *clscn = (close_connection *)blkptr;
            clscn->type = atoi(PQgetvalue(res, 0, 0));
            clscn->ipaddress = atoi(PQgetvalue(res, 0, 1));
            clscn->fd = atoi(PQgetvalue(res, 0, 2));
            clscn->scommid = atoi(PQgetvalue(res, 0, 3));


            memcpy(scommid, PQgetvalue(res, 0, 3), PQgetlength(res, 0, 3));
            const char *param_values[] = {scommid};
            const int paramLengths[] = {sizeof(scommid)};
            const int paramFormats[] = {0};
            int resutlFormat = 0;

            PQclear(res);
            res = PQexecPrepared(connection, dbs[7].statement_name, dbs[7].param_count, param_values, paramLengths, paramFormats, resutlFormat);
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                storelog("%s%s", "failed to update comms status : ", PQerrorMessage(connection));
                PQclear(res);
                return -1;
            }
            
        }
        else {
            storelog("%s", "invalid type of message received in processors retrive comms from db");
            PQclear(res);
            return -1;
        }
    } 
    else {
        PQclear(res);
        return -1;
    }
    
    PQclear(res);
    return  0;
}


// gives data to sender
void give_data_to_sender () 
{
    int subblock_position = -1;
    char *blkptr = NULL;

    sem_wait(sem_lock_datas.var);         
    subblock_position = get_subblock(datas_block.var, 0, 3);
    
    if (subblock_position >= 0) {

        blkptr = datas_block.var + (TOTAL_PARTITIONS/8) + subblock_position * DPARTITION_SIZE;
        memset(blkptr, 0, DPARTITION_SIZE);
        
        if (retrive_data_from_database(blkptr) == 0) { 
            toggle_bit(subblock_position, datas_block.var);
            sem_post(sem_lock_sigs.var);
        }
    }

    sem_post(sem_lock_datas.var);
    
}


// gives message to sender
void send_msg_to_sender () 
{
    int subblock_position = -1;
    char *blkptr = NULL;

    sem_wait(sem_lock_comms.var);         
    subblock_position = get_subblock(comms_block.var, 0, 1);
    
    if (subblock_position >= 0) {
       
        blkptr = comms_block.var + (TOTAL_PARTITIONS/8) + subblock_position*CPARTITION_SIZE;
        memset(blkptr, 0, CPARTITION_SIZE);
        
        if (retrive_comms_from_database(blkptr) == 0) {
            toggle_bit(subblock_position, comms_block.var);
            sem_post(sem_lock_sigs.var);
        }
        
    }
    
    sem_post(sem_lock_comms.var);
}


// read message from sender
void read_msg_from_sender () 
{
    int subblock_position = -1;
    char *blkptr = NULL;
    int attempts = 3;
    int status = 0;

    sem_wait(sem_lock_comms.var);         
    subblock_position = get_subblock(comms_block.var, 1, 2);
    
    if (subblock_position >= 0) {

        do {
            blkptr = comms_block.var + (TOTAL_PARTITIONS/8) + subblock_position*CPARTITION_SIZE;
            status = store_comms_into_database(blkptr);
            attempts -= 1;
             
            if (attempts > 0 && status == -1) {
                nanosleep(&tm, NULL);
            }

        } while (attempts > 0 && status == -1);
        
        toggle_bit(subblock_position, comms_block.var);
    }
    sem_post(sem_lock_comms.var);   
}


int run_process () 
{   
    // main run loop
    while (1) {
        // wait on semaphore
        // if there is some message for sender, database will send a notification
        // the notification will be read by the notif program and it will increment the value of semaphore
        // or
        // if sender wants to send some message, then sender will increment the value of semaphore
        sem_wait(sem_lock_sigps.var);
        read_msg_from_sender();
        send_msg_to_sender();
        give_data_to_sender();
    }
}


// connects to database
int connect_to_database (char *conninfo) 
{   
    connection = PQconnectdb(conninfo);
    if (PQstatus(connection) != CONNECTION_OK) {
        syslog(LOG_NOTICE, "Connection to database failed: %s\n", PQerrorMessage(connection));
        return -1;
    }
    return 0;
}


// prepare sql queries
int prepare_statements () 
{    
    int i, status = 0;
    for (i = 0; i<statement_count; i++){
        PGresult* res = PQprepare(connection, dbs[i].statement_name, dbs[i].statement, dbs[i].param_count, NULL);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            syslog(LOG_NOTICE, "Preparation of statement failed: %s\n", PQerrorMessage(connection));
            status = -1;
            break;
        }
        PQclear(res);
    }
    return status;
}


int main (int argc, char *argv[]) 
{
    int conffd = -1;
    char buf[500];
    char db_conn_command[100];
    char username[30];
    char dbname[30];

    memset(buf, 0, sizeof(buf));
    memset(db_conn_command, 0, sizeof(db_conn_command));
    memset(username, 0, sizeof(username));
    memset(dbname, 0, sizeof(dbname));

    if (argc != 2) {
        syslog(LOG_NOTICE,"invalid arguments");
        return -1;
    }

    // open the configuration file
    if ((conffd = open(argv[1], O_RDONLY)) == -1) {
        syslog(LOG_NOTICE, "failed to open configuration file");
        return -1;
    }

    // read the configuration file
    if (read(conffd, buf, sizeof(buf)) > 0) {
        sscanf(buf, "SEM_LOCK_DATAS=%s\nSEM_LOCK_COMMS=%s\nSEM_LOCK_SIG_S=%s\nSEM_LOCK_SIG_PS=%s\nPROJECT_ID_DATAS=%d\nPROJECT_ID_COMMS=%d\nUSERNAME=%s\nDBNAME=%s", sem_lock_datas.key, sem_lock_comms.key, sem_lock_sigs.key, sem_lock_sigps.key, &datas_block.key, &comms_block.key, username, dbname);
    }
    else {
        syslog(LOG_NOTICE, "failed to read configuration file");
        return -1;
    }

    close(conffd);

    // create command that will connect to database
    snprintf(db_conn_command, sizeof(db_conn_command), "user=%s dbname=%s", username, dbname);
    
    if (connect_to_database(db_conn_command) == -1) { return -1; }
    if (prepare_statements() == -1) { return -1; }   
    
    sem_lock_datas.var = sem_open(sem_lock_datas.key, O_CREAT, 0777, 1);
    sem_lock_comms.var = sem_open(sem_lock_comms.key, O_CREAT, 0777, 1);
    sem_lock_sigs.var = sem_open(sem_lock_sigs.key, O_CREAT, 0777, 0);
    sem_lock_sigps.var = sem_open(sem_lock_sigps.key, O_CREAT, 0777, 0);

    if (sem_lock_sigps.var == SEM_FAILED || sem_lock_sigs.var == SEM_FAILED || sem_lock_datas.var == SEM_FAILED || sem_lock_comms.var == SEM_FAILED) {
        storelog("%s", " processor s failed to initialize locks");
        return -1;
    }
        
    datas_block.var = attach_memory_block(FILENAME_S, DATA_BLOCK_SIZE, (char)datas_block.key);
    comms_block.var = attach_memory_block(FILENAME_S, COMM_BLOCK_SIZE, (char)comms_block.key);

    if (!( datas_block.var && comms_block.var)) {
        storelog("%s", "processor s failed to get shared memory");
        return -1; 
    }

    unset_all_bits(comms_block.var, 2);
    unset_all_bits(comms_block.var, 3);
    unset_all_bits(datas_block.var, 1);
    
    run_process(); 
    PQfinish(connection);  

    sem_close(sem_lock_datas.var);
    sem_close(sem_lock_comms.var);
    sem_close(sem_lock_sigs.var);
    sem_close(sem_lock_sigps.var);

    detach_memory_block(datas_block.var);
    detach_memory_block(comms_block.var);

    destroy_memory_block(datas_block.var, datas_block.key);
    destroy_memory_block(comms_block.var, comms_block.key);

    return 0;

}   