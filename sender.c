#include "sender.h"

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


int create_connection(unsigned short int port_number, int ip_address) 
{    
    int network_socket; // to hold socket file descriptor
    int connection_status; // to show wether a connection was established or not
    struct sockaddr_in server_address; // create a address structure to store the address of remote connection
    /* 
        create a socket
       first parameter is domain of the socket = AF_INET represents that it is an internet socket
       second parameter is type of socket = SOCK_STREAM represents that it is a TCP sockets
       third parametets specifies that protocol that is being used, since we want to use default i.e tcp
       hence it will be set to 0 
    */
    network_socket = socket(AF_INET, SOCK_STREAM, 0);
    
    // address of family 
    server_address.sin_family = AF_INET;    
    
    /*  
        port number
        htons converts the unsigned short from host byte order to network byte order
        host byte order is mainly big-endian, 
        while local machine may have little-endian byte order 
    */
    server_address.sin_port = htons(port_number);  

    /*  
        actual server address i.e Ip address 
        sin_addr is a structure that contains the field to hold the address
        
        in order to connect to local machine address can be 0000 or constant INADDR_ANY
    */
    server_address.sin_addr.s_addr = htonl(ip_address); 

    // if not able to send a message in mentioned amount of seconds, 
    // then it means there is some issue over the network
    // so it will close the connection

    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(network_socket, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));

    /*
        function to connect to remote machine
        first paratmeter is our socket
        second paratmeter is address structure, cast it to a different structure 
        third parameter is size of address structure

        connect returns a integer that will indicate wether connection was succesfull or not
    */
    connection_status = connect(network_socket, (struct sockaddr *)&server_address, sizeof(server_address));
    return (connection_status == -1 ? connection_status : network_socket);
      
}

// gets messages from processor
int get_message_from_processor(char *data) 
{
    int subblock_position = -1;
    char *blkptr;

    sem_wait(sem_lock_comms.var);         
    subblock_position = get_subblock(comms_block.var, 1, 1);
    
    if (subblock_position >= 0) {

        blkptr = comms_block.var + (TOTAL_PARTITIONS/8) + subblock_position * CPARTITION_SIZE;
        memset(data, 0, CPARTITION_SIZE);
        memcpy(data, blkptr, CPARTITION_SIZE);
        toggle_bit(subblock_position, comms_block.var);

    }
    sem_post(sem_lock_comms.var);
    return subblock_position;       
}

// gets data from processor
int get_data_from_processor(send_message *sndmsg)
{
    int subblock_position = -1;
    char *blkptr;
    
    sem_wait(sem_lock_datas.var);         
    subblock_position = get_subblock(datas_block.var, 1, 3);
    
    if (subblock_position >= 0) {

        blkptr = datas_block.var + (TOTAL_PARTITIONS/8) + subblock_position * DPARTITION_SIZE;
        memcpy(sndmsg, blkptr, sizeof(send_message));
        toggle_bit(subblock_position, datas_block.var);
    
    }

    sem_post(sem_lock_datas.var);

    return subblock_position;
}


// send message about status of data and connection status
void send_message_to_processor(int type, void *msg) 
{
    int subblock_position = -1;
    int attempts = 0;
    char *blkptr = NULL;

    sem_wait(sem_lock_comms.var);
    do {         
        subblock_position = get_subblock(comms_block.var, 0, 2);
        
        if (subblock_position >= 0) {

            blkptr = comms_block.var + (TOTAL_PARTITIONS/8) + subblock_position * CPARTITION_SIZE;
            memset(blkptr, 0, CPARTITION_SIZE);

            if (type == 3) {
                // type 3 represents communication related to connection status
                memcpy(blkptr, (connection_status *)msg, sizeof(open_connection));
            }
            else if(type == 4) {
                // type 4 represents communication related to message status
                memcpy(blkptr, (message_status *)msg, sizeof(message_status));
            }
            toggle_bit(subblock_position, comms_block.var);
            sem_post(sem_lock_sigps.var);
        }
        attempts -= 1;
        
        if (attempts > 0 && subblock_position == -1) {
            nanosleep(&tm, NULL);
        }

    } while(attempts > 0 && subblock_position == -1);

    sem_post(sem_lock_comms.var);
    
}


int run_sender() 
{
    int data_sent;
    int total_data_sent;
    char data[CPARTITION_SIZE];
    open_connection *opncn; 
    close_connection *clscn;
    message_status msgsts;
    send_message sndmsg;
    connection_status cncsts;
   
    while (1) {

        // wait on semaphore
        sem_wait(sem_lock_sigs.var);
        if (get_message_from_processor(data) != -1) {
            
            memset(&cncsts, 0, sizeof(connection_status));

            if (*(int *)data == 1) {
                // type 1 represents message to open a connection
                opncn = (open_connection *)data;
            
                // open connection on mentioned port and ipaddress
                // create a new message for processor that will convey the status of connection
                cncsts.fd = create_connection(opncn->port, opncn->ipaddress);
                cncsts.type = 3;
                cncsts.ipaddress = opncn->ipaddress;
                cncsts.scommid = opncn->scommid;
                
                send_message_to_processor(3, (void *)&cncsts);

            }
            else if(*(int *)data == 2) {
                // type 2 represents message to close a connection
                clscn = (close_connection *)data;
                close(clscn->fd);
                cncsts.type = 3;
                cncsts.fd = -1;
                cncsts.ipaddress = clscn->ipaddress;
                cncsts.scommid = clscn->scommid;
            
                send_message_to_processor(3, (void *)&cncsts);
            }
        }

        memset(&sndmsg, 0, sizeof(send_message));
        // get data from processor
        if (get_data_from_processor(&sndmsg)!= -1) {

            memset(&msgsts, 0, sizeof(message_status));
            data_sent = 0;
            total_data_sent = 0;
            
            // send the data over the network to remote machine
            do {
                data_sent = send(sndmsg.fd, sndmsg.data+total_data_sent, sndmsg.size, 0);
                total_data_sent += data_sent;
            } while (total_data_sent < sndmsg.size && data_sent != 0);
            
            // create a message for processor to convet the status of data that was sent
            msgsts.status = total_data_sent < sndmsg.size ? 0 : 1;
            msgsts.type = 4;
            memcpy(msgsts.uuid, sndmsg.uuid, sizeof(sndmsg.uuid));
            msgsts.uuid[36] = '\0';
           
            send_message_to_processor(4, (void *)&msgsts);
        }
    }
}


// connect to database
int connect_to_database(char *conninfo) 
{   
    connection = PQconnectdb(conninfo);
    if (PQstatus(connection) != CONNECTION_OK) {
        syslog(LOG_NOTICE, "Connection to database failed: %s\n", PQerrorMessage(connection));
        return -1;
    }
    return 0;
}

// prepare sql queries
int prepare_statements() 
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


int main(int argc, char *argv[]) 
{
    int conffd = -1;
    char buf[500];
    char db_conn_command[100];
    char username[30];
    char dbname[30];

    if (argc != 2) {
        syslog(LOG_NOTICE,"invalid arguments");
        return -1;
    }

    // open config file
    if ((conffd = open(argv[1], O_RDONLY)) == -1) {
        syslog(LOG_NOTICE, "failed to open configuration file");
        return -1;
    }

    // read the config file
    if (read(conffd, buf, sizeof(buf)) > 0) {
    
        sscanf(buf, "SEM_LOCK_DATAS=%s\nSEM_LOCK_COMMS=%s\nSEM_LOCK_SIG_S=%s\nSEM_LOCK_SIG_PS=%s\nPROJECT_ID_DATAS=%d\nPROJECT_ID_COMMS=%d\nUSERNAME=%s\nDBNAME=%s", sem_lock_datas.key, sem_lock_comms.key, sem_lock_sigs.key, sem_lock_sigps.key, &datas_block.key, &comms_block.key, username, dbname);
    }
    else {
        syslog(LOG_NOTICE, "failed to read configuration file");
        return -1;
    }

    close(conffd);

    sprintf(db_conn_command, "user=%s dbname=%s", username, dbname);
    if (connect_to_database(db_conn_command) == -1) { return -1; }
    if (prepare_statements() == -1) { return -1; }   

    sem_lock_datas.var = sem_open(sem_lock_datas.key, O_CREAT, 0777, 1);
    sem_lock_comms.var = sem_open(sem_lock_comms.key, O_CREAT, 0777, 1);
    sem_lock_sigs.var = sem_open(sem_lock_sigs.key, O_CREAT, 0777, 1);
    sem_lock_sigps.var = sem_open(sem_lock_sigps.key, O_CREAT, 0777, 1);

    if (sem_lock_sigs.var == SEM_FAILED || sem_lock_sigps.var == SEM_FAILED || sem_lock_datas.var == SEM_FAILED || sem_lock_comms.var == SEM_FAILED) {
        storelog("%s", "sender not able to initialize locks");
        return -1;
    }
 
    datas_block.var = attach_memory_block(FILENAME_S, DATA_BLOCK_SIZE, datas_block.key);
    comms_block.var = attach_memory_block(FILENAME_S, COMM_BLOCK_SIZE, comms_block.key);

    if (!(datas_block.var && comms_block.var)) { 
        storelog("%s", "sender not able to attach shared memory");
        return -1; 
    }

    run_sender();

    PQfinish(connection);  

    sem_close(sem_lock_datas.var);
    sem_close(sem_lock_comms.var);
    sem_close(sem_lock_sigs.var);
    sem_close(sem_lock_sigps.var);
    
    detach_memory_block(datas_block.var);
    detach_memory_block(comms_block.var);
    
    return 0;
}