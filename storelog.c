#include <stdarg.h>
#include "partition.h"

void storelog (char *fmt, ...) 
{
    int d;
    char c, *s;
    float f;
    char log[1000];
    char temp[1000];
    PGresult *res;
    va_list args;
    va_start(args, fmt);

    memset(log, 0, sizeof(log));

        // based on input type, perform operation
        // %c represents character
        // %s represents string (array of characters)
        // %d represents integer
        // %f represents float
        // all values are evaluated based on the basis of alphabetic character
        // '%' symbol is not evaluated so no need to use it
        
    while (*fmt) {

        switch (*fmt++) {
            
            case 'c':
                    memset(temp, 0, sizeof(temp));
                    c = (char )va_arg(args, int);
                    snprintf(temp, sizeof(temp),"%c", c);
                    strncat(log, temp, sizeof(temp));
                    break;
            
            case 's':
                    s = va_arg(args, char *);
                    strncat(log, s, sizeof(temp));
                    break;
            
            case 'd':
                    memset(temp, 0, sizeof(temp));
                    d = va_arg(args, int);
                    snprintf(temp, sizeof(temp),"%d", d);
                    strncat(log, temp, sizeof(temp));
                    break;

            case 'f':
                    memset(temp, 0, sizeof(temp));
                    f = va_arg(args, double);
                    snprintf(temp, sizeof(temp),"%lf", f);
                    strncat(log, temp, sizeof(temp));
                    break;
            default:
                break;
        };
    }

    va_end(args);
    log[999] = '\0';

    const char *const param_values[] = {log};
    const int paramLengths[] = {sizeof(log)};
    const int paramFormats[] = {0};
    int resultFormat = 0;
    
    // execute the insert log query
    // the query will be prepared by the process that will use this function
    res = PQexecPrepared(connection, "storelog", 1, param_values, paramLengths, paramFormats, resultFormat);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        syslog(LOG_NOTICE, "logging failed %s , log %s\n", PQerrorMessage(connection), log);
    }

    PQclear(res);
}

