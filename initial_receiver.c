#include <sys/socket.h> // contains important fucntionality and api used to create sockets
#include <sys/types.h>  // contains various types required to create a socket   
#include <netinet/in.h> // contains structures to store address information
#include <arpa/inet.h>
#include <sys/types.h>
#include "partition.h"
#include "initial_receiver.h"

server_info s_info; // global variable to connection info, see initial_server.h for more info
PGconn *connection; // global variable points at the connection structure that represents connection to db

int store_data_in_database(int client_socket, char *data) {

    PGresult *res;

    char fd[11];
    sprintf(fd, "%d", client_socket);
    
    // necessary parameters for PQexecPrepared statment
    const char *const param_values[] = {fd, data};
    const int paramLengths[] = {sizeof(fd), strlen(data)};
    const int paramFormats[] = {0, 1};
    int resultFormat = 0;
    
    res = PQexecPrepared(connection, dbs[1].statement_name, dbs[1].param_count, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s%s", "Message storing failed in initial receiver : ", PQerrorMessage(connection));
        PQclear(res);
        return -1;
    }

    PQclear(res);
    return 0;
}


void run_server() {
    
    int client_socket, data_read, total_data_read;
    char data[IRMESSAGE_SIZE+1];
    int attempts = 3;
    int status = 0;
    struct sockaddr_in server_address; 
    struct sockaddr_in client_address;
    socklen_t addr_len = sizeof(client_address);

    if ((s_info.servsoc_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        storelog("%s%s", "intial receiver failed to create a socket for listening : ", strerror(errno));
        return;
    }
    
    server_address.sin_family = AF_INET;    
    server_address.sin_port = htons(s_info.port);  
    server_address.sin_addr.s_addr = htonl(s_info.ipaddress); 

    if (bind(s_info.servsoc_fd, (struct sockaddr *)&server_address, sizeof(server_address)) == -1) {
        storelog("%s%s", "initial receiver failed to bind : ", strerror(errno));
        return;
    }

    if (listen(s_info.servsoc_fd, 100) == -1) {
        storelog("%s%s", "initial receiver failed to listen : ", strerror(errno));
        return;
    }  

    while (1) {
        
        // acccept a incoming client connection
        client_socket = accept(s_info.servsoc_fd, (struct  sockaddr *)&client_address, &addr_len);
       
        if (client_socket >= 0) {
            
            memset(data, 0, sizeof(data));
            data_read = total_data_read = 0;

            // read until the full message size(IRMESSAGE_SIZE) is not read or it fails to read any data (0 bytes)
            do {    

                data_read = read(client_socket, data+total_data_read, IRMESSAGE_SIZE);     
                total_data_read += data_read;

            } while(total_data_read < IRMESSAGE_SIZE && data_read > 0);

            if (total_data_read != IRMESSAGE_SIZE) {
                storelog("%s%d%s%d", "data receiving failure size : ", total_data_read, ", fd : ",client_socket);
            }
            else {
                // try storing data in database, if failed try 3 times
                attempts = 3;
                do {
                    status = store_data_in_database(client_socket, data);
                    attempts -= 1;
                } while (attempts > 0 && status == -1);
            }
            close(client_socket);
        }
        else {
            storelog("%s%s", "inital receiver failed to accept client connection : ", strerror(errno));
        }
    }
    
    close(s_info.servsoc_fd);
    
}


// creates connection to database
int connect_to_database(char *conninfo) 
{   
    connection = PQconnectdb(conninfo);
    if (PQstatus(connection) != CONNECTION_OK) {
        syslog(LOG_NOTICE, "Connection to database failed: %s\n", PQerrorMessage(connection));
        return -1;
    }

    return 0;
}

// sql statements, these statements will be executed by various fucnction to interact with db
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
    char buf[100];
    char db_conn_command[100];
    char username[30];
    char dbname[30];

    if (argc != 2) {
        syslog(LOG_NOTICE,"invalid arguments");
        return -1;
    }
   
    // open the config file
    if ((conffd = open(argv[1], O_RDONLY)) == -1) {
        syslog(LOG_NOTICE, "failed to open configuration file");
        return -1;
    }

    // read the data from config file
    memset(buf, 0, sizeof(buf));
    if (read(conffd, buf, sizeof(buf)) > 0) {
       
        sscanf(buf, "USERNAME=%s\nDBNAME=%s\nPORT=%d\nIPADDRESS=%lu", username, dbname, &s_info.port, &s_info.ipaddress);
    }
    else {
        syslog(LOG_NOTICE, "failed to read configuration file");
        return -1;
    }

    close(conffd);

    memset(db_conn_command, 0, sizeof(db_conn_command));
    
    // create query to connect to database
    sprintf(db_conn_command, "user=%s dbname=%s", username, dbname);

    if (connect_to_database(db_conn_command) == -1) { return -1; }
    if (prepare_statements() == -1) { return -1; }     

    // main run loop
    run_server();
    PQfinish(connection);

    return 0;
}
