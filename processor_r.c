#include "processor_r.h"

PGconn *connection;
semlocks sem_lock_datar;
semlocks sem_lock_commr;
semlocks sem_lock_sigr;
datablocks datar_block;
datablocks commr_block;

const struct timespec tm = {
    .tv_sec = 1,
    .tv_nsec = 0L,
};

int store_data_in_database (newmsg_data *nmsg) 
{
    PGresult *res;
    
    char fd[11];

    sprintf(fd, "%d", nmsg->data1);

    const int paramLengths[] = {sizeof(nmsg->data1), nmsg->data2};
    const int paramFormats[] = {0, 1};
    int resultFormat = 0;

    const char *const param_values[] = {fd, nmsg->data};
    
    res = PQexecPrepared(connection, dbs[0].statement_name, 
                                    dbs[0].param_count, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s %s", "data storing failed", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    PQclear(res);

    return 0;
}


int store_commr_into_database (receivers_message *rcvm) 
{
    PGresult *res;

    char fd[11];
    char ipaddr[11];
    char status [11];
   
    sprintf(fd, "%d", rcvm->fd);
    sprintf(ipaddr, "%d", rcvm->ipaddr);
    sprintf(status, "%d", rcvm->status);

    const int paramLengths[] = {sizeof(fd), sizeof(ipaddr), sizeof(status)};
    const int paramFormats[] = {0, 0, 0};
    int resultFormat = 0;
   
    const char *const param_values[] = {fd, ipaddr, status};
    
    res = PQexecPrepared(connection, dbs[1].statement_name, dbs[1].param_count, param_values, paramLengths, paramFormats, resultFormat);
   
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s%s", "comms storing failed : ", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    PQclear(res);

    return 0;
}


// gets data from receiver and strores in database
void get_data_from_receiver () 
{
    int subblock_position = -1;
    char *blkptr = NULL;
    newmsg_data nmsg;
    int status = 0;
    int attempts = 3;

    // wait on semaphore i.e try to decrement it
    sem_wait(sem_lock_datar.var);

    // get_subblock function checks bitmap to see if there is any bit that set
    // if a bit is set, it means there is data that needs to be read
    // get_subblock returns the position number of block         
    subblock_position = get_subblock(datar_block.var, 1, 3);
    
    // if subblock value > 0, it means got a proper subblock
    // otherwise values < 0 indicates no subblock available to read
    if (subblock_position >= 0) {

        do {
            // take the pointer to appropriate location
            // datar_block.var represents the starting position of block
            // TOTAL_PARTITIONS/8 represents the size of bitmap
            // since bitmap is present at begining of block, it needs to be ignored
            // subblock_position represents the position number of subblock
            // DPARTITION_SIZE represents the size of individual subblock

            blkptr = datar_block.var + (TOTAL_PARTITIONS/8) + subblock_position * DPARTITION_SIZE;
            memset(&nmsg, 0, sizeof(nmsg));
            memcpy(&nmsg, blkptr, sizeof(nmsg));
            
            // try to store data in databse
            status = store_data_in_database(&nmsg);
            attempts -= 1;

            // if status == -1, which indicates failure and there are attempts remaining
            // then go to sleep, as there is going to be reattempt to fetch the data 
            if (attempts > 0 && status == -1) {
                nanosleep(&tm, NULL);
            }

            // based on status and remaining attempts take appropriate action
        } while (attempts > 0 && status == -1);
        
        // toggle the bit corresponding to that subblock 
        toggle_bit(subblock_position, datar_block.var);
    }
    // increment semaphore value
    sem_post(sem_lock_datar.var);
}

// gets messages from receiver and stores them in database
void get_message_from_receiver () 
{
    int subblock_position = -1;
    char *blkptr = NULL;
    int status = 0;
    int attempts = 3;
    receivers_message rcvm;

    sem_wait(sem_lock_commr.var);         
    subblock_position = get_subblock(commr_block.var, 1, 2);
    
    if (subblock_position >= 0) {

        do {
            blkptr = commr_block.var + (TOTAL_PARTITIONS/8) + subblock_position*CPARTITION_SIZE;
            memset(&rcvm, 0, sizeof(rcvm));
            memcpy(&rcvm, blkptr, sizeof(rcvm));
            status = store_commr_into_database(&rcvm);
            attempts -= 1;
            
            if (attempts > 0 && status == -1) {
                nanosleep(&tm, NULL);
            }
            
        } while (attempts > 0 && status == -1);
            
        toggle_bit(subblock_position, commr_block.var);
    }
    sem_post(sem_lock_commr.var);

}

// retrives necessary messages from database, that are intended for receiver
int get_comms_from_database (char *blkptr) 
{
    PGresult *res;
    capacity_info cpif;
    
    res = PQexecPrepared(connection, dbs[3].statement_name, dbs[3].param_count, NULL, NULL, NULL, 0);
   
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        storelog("%s%s", "retriving comms for receiver failed : ", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }    
    else if (PQntuples(res) > 0) {
    
        memset(&cpif, 0, sizeof(capacity_info));
        memcpy(cpif.ipaddress, PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
        cpif.ipaddress[sizeof(cpif.ipaddress)] = 0;
        cpif.capacity = atoi(PQgetvalue(res, 0, 1));
        memcpy(blkptr, &cpif, sizeof(capacity_info));
        PQclear(res);
        return 0;
    }

    PQclear(res);
    return -1;
}

// send necessary messages to receiver
void send_msg_to_receiver () 
{
    int subblock_position;
    char *blkptr = NULL;
    
    sem_wait(sem_lock_commr.var);
    subblock_position = get_subblock(commr_block.var, 0, 1);

    if (subblock_position >= 0) {

        blkptr = commr_block.var + (TOTAL_PARTITIONS/8) + subblock_position*CPARTITION_SIZE;
        memset(blkptr, 0, CPARTITION_SIZE);
   
        if (get_comms_from_database(blkptr) == 0) {
            toggle_bit(subblock_position, commr_block.var);
        }
    }   

    sem_post(sem_lock_commr.var);
} 


int run_process () 
{   
    while (1) {
        // wait on semaphore
        // if there is some message for receiver, database will send a notification
        // the notification will be read by the notif program and it will increment the value of semaphore
        // or
        // if receiver wants to send some data or message, then receiver will increment the value of semaphore
        sem_wait(sem_lock_sigr.var);    
        get_message_from_receiver();
        get_data_from_receiver();     
        send_msg_to_receiver();
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


// prepares all the necessary sql queries
int prepare_statements () 
{    
    int i, status = 0;

    for (i = 0; i<statement_count; i++){

        PGresult* res = PQprepare(connection, dbs[i].statement_name, dbs[i].statement, dbs[i].param_count, NULL);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            syslog(LOG_NOTICE, "Preparation of statement failed: %s\n", PQerrorMessage(connection));
            status = -1;
            PQclear(res);
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
       
        sscanf(buf, "SEM_LOCK_DATAR=%s\nSEM_LOCK_COMMR=%s\nSEM_LOCK_SIG_R=%s\nPROJECT_ID_DATAR=%d\nPROJECT_ID_COMMR=%d\nUSERNAME=%s\nDBNAME=%s", sem_lock_datar.key, sem_lock_commr.key, sem_lock_sigr.key, &datar_block.key, &commr_block.key, username, dbname);
    }
    else {
        syslog(LOG_NOTICE, "failed to read configuration file");
        return -1;
    }
    
    close(conffd);

    // create the command that will connect to database
    snprintf(db_conn_command, sizeof(db_conn_command), "user=%s dbname=%s", username, dbname);
    
    if (connect_to_database(db_conn_command) == -1) { return -1; }
    if (prepare_statements() == -1) { return -1; }   
    
    // open necessary semaphores andd attach them to appropriate variables
    sem_lock_datar.var = sem_open(sem_lock_datar.key, O_CREAT, 0777, 1);
    sem_lock_commr.var = sem_open(sem_lock_commr.key, O_CREAT, 0777, 1);
    sem_lock_sigr.var = sem_open(sem_lock_sigr.key, O_CREAT, 0777, 0);
    
    if (sem_lock_sigr.var == SEM_FAILED || sem_lock_datar.var == SEM_FAILED || sem_lock_commr.var == SEM_FAILED) {
        storelog("%s"," processor r failed to intialize locks");
        return -1;
    }

    // attach varaibles to shared memory blocks
    datar_block.var = attach_memory_block(FILENAME_R, DATA_BLOCK_SIZE, datar_block.key);
    commr_block.var = attach_memory_block(FILENAME_R, COMM_BLOCK_SIZE, commr_block.key);
    if (!(datar_block.var && commr_block.var)) {
        storelog("%s", " procsssor s failed to get shared memory");
        return -1; 
    }

    // unset all bits in bitmaps representing the shared memory blocks
    unset_all_bits(commr_block.var, 2);
    unset_all_bits(commr_block.var, 3);
    unset_all_bits(datar_block.var, 1);
    
    // call the man run loop
    run_process(); 
    
    PQfinish(connection);    

    sem_close(sem_lock_datar.var);
    sem_close(sem_lock_commr.var);
    sem_close(sem_lock_sigr.var);
    
    detach_memory_block(datar_block.var);
    detach_memory_block(commr_block.var);
    
    destroy_memory_block(datar_block.var, datar_block.key);
    destroy_memory_block(commr_block.var, commr_block.key);
    
    return 0;
}   
