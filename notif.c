//#include <sys/epoll.h>
#include<poll.h>
#include "shared_memory.h"
#include "partition.h"


/*
	--Changes made so far to the file
	1.commented the epoll statements.
	2.added the poll include statement
	3.declare constructs required by the poll library to funtion.
	4.made changes to the main algorithm to accomodate the poll.
*/

int dbsocket;
//int epoll_fd;
char noti_channel[30];
//struct epoll_event event;
PGconn *connection;
semlocks sem_lock_sig;
void storelog(char * fmt, ...);

/* for use with poll on systems where epoll is available*/

struct pollfd pfd;


int initnotif(char *confg_filename) 
{
    int conffd = -1;
    char buf[500];
    char username[30];
    char dbname[30];
    char noti_command[100];
    char db_conn_command[100];

    PGresult *res = NULL;

    // open configuration file
    if ((conffd = open(confg_filename, O_RDONLY)) == -1) {
        syslog(LOG_NOTICE, "failed to open configuration file");
        return -1;
    }

    memset(buf, 0, sizeof(buf));
    memset(noti_channel, 0 , sizeof(noti_channel));
    memset(db_conn_command, 0, sizeof(db_conn_command));
    memset(username, 0, sizeof(username));
    memset(dbname, 0, sizeof(dbname));

    // read the configuration file
    if (read(conffd, buf, sizeof(buf)) > 0) {
        sscanf(buf,"SEM_LOCK_SIG=%s\nUSERNAME=%s\nDBNAME=%s\nNOTI_CHANNEL=%s",  sem_lock_sig.key, username, dbname, noti_channel);
    }
    else {
        syslog(LOG_NOTICE, "failed to read configuration file");
        return -1;
    }

    // create query to listen on socket 
    sprintf(noti_command, "LISTEN %s", noti_channel);
    
    // create query to connect to database
    sprintf(db_conn_command, "user=%s dbname=%s", username, dbname);

    // connect to database
    connection = PQconnectdb(db_conn_command);
    if (PQstatus(connection) != CONNECTION_OK) {
        syslog(LOG_NOTICE, "failed to create connection to database user %s dbname %s noti_channel %s error %s", username, dbname, noti_channel, PQerrorMessage(connection));
        return -1;
    }

    // prepare query to store logs
    res = PQprepare(connection, "store_log", "INSERT INTO logs (log) VALUES ($1);", 1, NULL);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        syslog(LOG_NOTICE, "Preparation of statement failed: %s\n", PQerrorMessage(connection));
        PQclear(res);
        PQfinish(connection);
        return -1;
    }

    // open a socket connection to db
    dbsocket = PQsocket(connection);
    if (dbsocket == -1) {
        storelog("%s%s", "noti channel : ", noti_channel, " error in PQsocket creating a connection to db : ", PQerrorMessage(connection));
        PQfinish(connection);
        return -1;
    }
    PQsetnonblocking(connection, 1);

    // create an epoll instance to check activity on socket
    /*
	epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        storelog("%s%s", "epoll_create1 failed in notifier listening on : ", noti_channel);
        PQfinish(connection);
        return -1;
    }
	
    event.events = EPOLLIN;
    event.data.fd = dbsocket;
	*/
	pfd.fd = dbsocket;
	pfd.events = POLLIN;
	/*
    // add the epoll file descriptor into epoll data structure, internally handeled by kernel
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, dbsocket, &event) == -1) {
        storelog("%s%s", "epollctl failed in notifier listening on : ", noti_channel);
        PQfinish(connection);
        close(epoll_fd);
        return -1;
    }
	*/
    PQclear(res);

    // execute query to listen on notification channel
    res = PQexec(connection, noti_command);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        storelog("%s%s%s%s", "noti channel : ", noti_channel, " LISTEN command failed : ", PQerrorMessage(connection));
        PQclear(res);
        //close(epoll_fd);
        PQfinish(connection);
        return -1;
    }
    PQclear(res);
    
    // open semaphore, attach variable to it
    if ((sem_lock_sig.var = sem_open(sem_lock_sig.key, O_CREAT, 0777, 0)) == SEM_FAILED) {
        storelog("%s%s%s%s", "noti channel : ", noti_channel,  " error creating semphore : ", sem_lock_sig.key, PQerrorMessage(connection));
        //close(epoll_fd);
        PQfinish(connection);
        return -1;
    }

    return 0;
}


int main(int argc, char *argv[]) 
{
    PGnotify *notify;
    int num_events;

    if (argc != 2) {
        syslog(LOG_NOTICE, "invlaid arguments");
    }
    
    // intialize required variables 
    if(initnotif(argv[1]) == -1) {return -1;}
    
    while (1) {
		num_events = poll( &pfd,1,-1);
		//if activity on the intended fd.
		if( (num_events == 1)&&((pfd.revents & POLLIN) == POLLIN) )  
		{
			
            if (PQconsumeInput(connection) == 0) 
			{
                storelog("%s%s%s%s", "noti channel : ", noti_channel ," Failed to consume input : ", PQerrorMessage(connection));
                break;
            }
            else 
			{
                // keep reading notifications untill there are none
                while ((notify = PQnotifies(connection)) != NULL) 
				{
                    // incerement the value of semaphore
                    sem_post(sem_lock_sig.var);
                    PQfreemem(notify);
                }
            }
		}
		//something went wrong log in system log.
		if( num_events == -1 )
		{
            storelog("%s%s", "poll wait failed for notifier listening on channel : ", noti_channel);
		}

	}
    sem_close(sem_lock_sig.var);
    PQfinish(connection);
	return 0;
}


        // wait until there is activity on port
		/*
        num_events = epoll_wait(epoll_fd, &event, 1, -1);
        if (num_events == -1) {
            storelog("%s%s", "epoll wait failed for notifier listening on channel : ", noti_channel);
            break;
        }
        // if activity is from intended socket then proceed
        if (event.data.fd == dbsocket) {
            // check if it can read from socket
            if (PQconsumeInput(connection) == 0) {
                storelog("%s%s%s%s", "noti channel : ", noti_channel ," Failed to consume input : ", PQerrorMessage(connection));
                break;
            }
            else {
                // keep reading notifications untill there are none
                while ((notify = PQnotifies(connection)) != NULL) {
                    // incerement the value of semaphore
                    sem_post(sem_lock_sig.var);
                    PQfreemem(notify);
                }
            }
        }
        else {
            storelog("%s%s", "unkown event occured on epoll of notifier listening on channel : ", noti_channel);
            break;
        }    
    }
    sem_close(sem_lock_sig.var);
    PQfinish(connection);
    close(epoll_fd);
	


    return 0;
}
*/
