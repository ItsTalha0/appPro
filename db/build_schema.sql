DROP VIEW IF EXISTS show_jobs_info;
DROP VIEW IF EXISTS show_stats;
DROP VIEW IF EXISTS show_files_info;
DROP TRIGGER IF EXISTS create_msg_receiver ON sysinfo;
DROP TABLE IF EXISTS logs, receivers_comms, receiving_conns, job_scheduler, sysinfo, 
                        senders_comms, sending_conns, files, selfinfo;
DROP FUNCTION IF EXISTS send_noti1(), send_noti2(), send_noti3(), create_comms(), run_jobs(), create_message(bytea, text, bytea, bytea, text, text, text, int);

UNLISTEN noti_1sender;
UNLISTEN noti_1receiver;
UNLISTEN noti_1initialsender;


CREATE TABLE job_scheduler (jobid UUID PRIMARY KEY, 
                            jobdata bytea NOT NULL,
                            jstate CHAR(5) NOT NULL DEFAULT 'N-1',
                            jtype TEXT NOT NULL DEFAULT '1',
                            jsource TEXT NOT NULL,
                            jparent_jobid UUID 
                                REFERENCES job_scheduler(jobid) 
                                ON UPDATE CASCADE 
                                ON DELETE CASCADE,
                            jdestination TEXT NOT NULL DEFAULT 0,
                            jpriority  TEXT NOT NULL DEFAULT '10',
                            jcreation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);


CREATE TABLE receivers_comms (rcomid SERIAL PRIMARY KEY, 
                              rdata1 BIGINT NOT NULL,
                              rdata2 BIGINT NOT NULL);

CREATE TABLE receiving_conns (rfd INTEGER PRIMARY KEY, 
                              ripaddr BIGINT NOT NULL, 
                              rcstatus INTEGER NOT NULL,
                              rctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE sending_conns (sfd INTEGER NOT NULL,
                            sipaddr BIGINT NOT NULL, 
                            scstatus INTEGER NOT NULL,
                            sctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            CONSTRAINT pk_sending_conns PRIMARY KEY (sipaddr));

CREATE TABLE senders_comms (scommid SERIAL, 
                            mdata1 BIGINT NOT NULL,
                            mdata2 INTEGER NOT NULL, 
                            mtype TEXT NOT NULL,
                            CONSTRAINT pk_senders_comms PRIMARY KEY (mdata1, mdata2));

CREATE TABLE logs (logid SERIAL PRIMARY KEY,
                   log TEXT NOT NULL,
                   lgtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE sysinfo (system_name CHAR(10),
                      ipaddress BIGINT UNIQUE,
                      dataport INTEGER,
                      comssport INTEGER NOT NULL,
                      system_capacity INTEGER,
                      CONSTRAINT pk_sysinfo PRIMARY KEY(system_name, ipaddress));

CREATE TABLE selfinfo (system_name CHAR(10) NOT NULL,
                      ipaddress BIGINT UNIQUE NOT NULL,
                      dataport INTEGER NOT NULL,
                      comssport INTEGER NOT NULL,
                      system_capacity INTEGER NOT NULL);


CREATE TABLE files (file_id UUID PRIMARY KEY, 
                        file_name TEXT UNIQUE NOT NULL,  
                        file_data oid NOT NULL, 
                        creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP); 


CREATE OR REPLACE VIEW show_stats 
AS 
SELECT count(jstate) AS total_jobs, jstate, jpriority, jdestination, jtype 
FROM job_scheduler 
GROUP BY jstate, jpriority, jdestination, jtype;


CREATE OR REPLACE VIEW show_jobs_info AS
SELECT  js1.jobid, encode(js1.jobdata, 'escape') AS file_name, js1.jstate, js1.jpriority, js1.jdestination 
FROM job_scheduler js1 
WHERE js1.jtype = lpad('6', 5, ' ') 
GROUP BY js1.jobid;

CREATE OR REPLACE VIEW show_files_info AS
SELECT * FROM FILES;

CREATE OR REPLACE FUNCTION create_message(
    uuid_data bytea,
    message_type text,
    subheader bytea,
    messaget bytea,
    message_source text,
    message_destination text,
    message_priority text,
    max_capacity int
) RETURNS bytea
AS
$$
DECLARE   
    hnmessage bytea;
    fixed_type text;
    fixed_source text;
    fixed_destination text;
    fixed_priority text;
    extra_pad text;
    size_difference int;
BEGIN
    fixed_type := lpad(message_type, 5, ' ');
    fixed_source := lpad(message_source, 5, ' ');
    fixed_destination := lpad(message_destination, 5, ' ');
    fixed_priority := lpad(message_priority, 5, ' ');
    
    hnmessage := uuid_data || fixed_type::bytea || 
                fixed_source::bytea || fixed_destination::bytea || 
                fixed_priority::bytea ||  to_char(now(), 'YYYY-MM-DD HH24:MI:SS.US')::bytea || subheader || messaget;

    extra_pad := '';
    size_difference := (max_capacity - 32) - length(hnmessage);

    IF size_difference > 0 THEN
        extra_pad := lpad(extra_pad, size_difference, ' ');  
        hnmessage := hnmessage || extra_pad::bytea;
    END IF;
    
    return md5(hnmessage)::bytea || hnmessage;

END;
$$
LANGUAGE 'plpgsql';


INSERT INTO 
    selfinfo 
VALUES(
        lpad('M3', 5, ' '), 
        2130706433, 
        6001,
        6000, 
        1024*32
);


INSERT INTO 
    job_scheduler(jobdata, jstate, jtype, jsource, 
    jobid, jparent_jobid, jdestination, jpriority) 
VALUES (
        '__ROOT__', 
        'N-0', 
        '0', 
        lpad('M3', 5, ' '), 
        GEN_RANDOM_UUID(), 
        NULL, 
        lpad('M3', 5, ' '), 
        0
);


UPDATE 
    job_scheduler 
SET 
    jparent_jobid = jobid 
WHERE 
    jobdata = '__ROOT__';


ALTER TABLE 
    job_scheduler 
ALTER COLUMN 
    jparent_jobid
SET NOT NULL;




CREATE OR REPLACE FUNCTION create_comms ()
RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO receivers_comms(rdata1, rdata2) VALUES (NEW.ipaddress, NEW.system_capacity);
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE TRIGGER
    create_msg_receiver
AFTER INSERT OR UPDATE ON
    sysinfo
FOR EACH ROW
WHEN
    (NEW.system_capacity != 0)
EXECUTE FUNCTION
    create_comms();



CREATE OR REPLACE FUNCTION send_noti2()
RETURNS TRIGGER AS 
$$
BEGIN
    PERFORM pg_notify('noti_1initialsender', gen_random_uuid()::TEXT);
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;



CREATE TRIGGER 
    msg_for_initial_sender
AFTER INSERT ON 
    job_scheduler
FOR EACH ROW 
WHEN 
    (NEW.jstate = 'S-5')
EXECUTE FUNCTION 
    send_noti2();


CREATE OR REPLACE FUNCTION send_noti1()
RETURNS TRIGGER AS 
$$
BEGIN
    PERFORM pg_notify('noti_1sender', gen_random_uuid()::TEXT);
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE TRIGGER 
    msg_for_sender1
AFTER UPDATE OR INSERT ON 
    job_scheduler
FOR EACH ROW 
WHEN 
    (NEW.jstate = 'S-4')
EXECUTE FUNCTION 
    send_noti1();


CREATE TRIGGER 
    msg_for_sender2
AFTER INSERT ON 
    senders_comms
FOR EACH ROW 
WHEN 
    (NEW.mtype in ('1','2'))
EXECUTE FUNCTION 
    send_noti1();


CREATE OR REPLACE FUNCTION send_noti3()
RETURNS TRIGGER AS
$$
BEGIN
    PERFORM pg_notify('noti_1receiver', gen_random_uuid()::TEXT);
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE TRIGGER
    msg_for_receiver
AFTER INSERT ON
    receivers_comms
FOR EACH ROW 
EXECUTE FUNCTION
    send_noti3();


CREATE OR REPLACE FUNCTION run_jobs ()
RETURNS VOID AS
$$
BEGIN

    
    UPDATE job_scheduler 
    SET jstate = 'N-2'
    WHERE jstate = 'N-1'
    AND encode(substr(jobdata, 1, 32), 'escape') = md5(substr(jobdata, 33));


    UPDATE job_scheduler 
    SET jstate = 'D'
    WHERE jstate = 'N-1'
    AND encode(substr(jobdata, 1, 32), 'escape') != md5(substr(jobdata, 33));
    

    UPDATE job_scheduler 
    SET jstate = 'N-3' 
    WHERE jstate = 'N-2' 
    AND encode(substr(jobdata, 79, 5), 'escape') IN (
            SELECT jdestination 
            FROM job_scheduler 
            WHERE jobid = jparent_jobid
        );


    UPDATE job_scheduler 
    SET jstate = 'S-1', jdestination = encode(substr(jobdata, 79, 5), 'escape') 
    WHERE jstate = 'N-2' 
    AND encode(substr(jobdata, 79, 5), 'escape') NOT IN (
            SELECT jdestination 
            FROM job_scheduler 
            WHERE jobid = jparent_jobid
        );


    UPDATE job_scheduler 
    SET jstate = 'N-4', jtype = encode(substr(jobdata, 69, 5), 'escape')
    WHERE jstate = 'N-3';


    UPDATE job_scheduler 
    SET jstate = 'C' 
    WHERE jstate = 'S-2W' 
    AND jobid IN (
            SELECT js1.jparent_jobid 
            FROM job_scheduler js1
            JOIN job_scheduler js2
            ON js1.jparent_jobid = js2.jparent_jobid
            WHERE encode(substr(js1.jobdata, 69, 5), 'escape') = lpad('3', 5, ' ')
            AND js2.jstate = 'C'
            GROUP BY js1.jparent_jobid, js1.jobdata        
            HAVING encode(substr(js1.jobdata, 163, 10), 'escape')::INTEGER + 1 = count(js2.jobid)
        );


    UPDATE job_scheduler
    SET jstate = 'C'
    WHERE jstate = 'S-3W'
    AND jobid IN (
            SELECT jparent_jobid
            FROM job_scheduler
            WHERE jstate = 'C'
        );


    WITH conn_info_sending AS (
            
        SELECT gen_random_uuid() AS uuid_data, sf.system_capacity, sf.system_name, sf.dataport,
            sf.ipaddress, js.jparent_jobid, si.system_name AS destination, 1, 1
        FROM selfinfo sf, sysinfo si 
        JOIN job_scheduler js 
        ON js.jdestination = si.system_name
        WHERE si.dataport = 0
        AND js.jstate = 'S' 
    )
    INSERT INTO job_scheduler (jobdata, jtype, jstate, jsource, jdestination, jpriority, jobid, jparent_jobid)
    SELECT create_message (

            uuid_data::text::bytea, '4'::text, 
            (lpad(system_capacity::text, 11, ' ') || (lpad(dataport::text, 11, ' ')) || (lpad(ipaddress::text, 11, ' ')))::bytea ,
            ''::bytea, system_name::text, destination::text, '5'::text, 147
        
        ), '4', 'S-5', system_name, destination, 5, uuid_data, jparent_jobid 
    FROM conn_info_sending;


    UPDATE job_scheduler 
    SET jstate = 'S-1'
    WHERE jdestination IN (
            SELECT system_name
            FROM sysinfo     
        )
    AND jstate = 'S';


    WITH conn_info_receiving AS (    

        SELECT 
            encode(substr(jobdata, 115, 11), 'escape')::INTEGER AS rcapacity,
            encode(substr(jobdata, 126, 11), 'escape')::INTEGER As rport,
            encode(substr(jobdata, 137, 11), 'escape')::INTEGER As source_ip,
            encode(substr(jobdata, 74, 5), 'escape') AS source_name
        FROM job_scheduler 
        WHERE jstate = 'N-4'
        AND jtype = lpad('4', 5, ' ')
    ),
    conn_info AS (   
            
        SELECT  source_name, source_ip, rport,
            (
                SELECT CASE 
                    WHEN rcapacity > selfinfo.system_capacity 
                    THEN selfinfo.system_capacity
                    ELSE rcapacity
                    END
                FROM conn_info_receiving, selfinfo
            ) data_capacity
        FROM conn_info_receiving
    )
    INSERT INTO sysinfo (system_name, ipaddress, dataport, comssport, system_capacity)
    SELECT lpad(source_name, 5, ' '), source_ip::BIGINT, rport, 7000, data_capacity
    FROM conn_info
    ON CONFLICT (system_name, ipaddress) 
    DO UPDATE 
    SET (system_capacity, dataport) = (
            SELECT ci.data_capacity, ci.rport
            FROM conn_info ci
            WHERE ci.source_name = sysinfo.system_name
            AND ci.source_ip::BIGINT = sysinfo.ipaddress
        );


    WITH conn_info_receiving AS ( 

        SELECT encode(substr(jobdata, 115, 11), 'escape')::INTEGER AS rcapacity,
            encode(substr(jobdata, 74, 5), 'escape') AS message_source, jparent_jobid
        FROM job_scheduler 
        WHERE jstate = 'N-4'
        AND jtype = lpad('4', 5, ' ')

    ),
    conn_info AS(
        SELECT gen_random_uuid() AS uuid_data, 
            message_source AS destination, 
            jparent_jobid,
            selfinfo.system_name, 
            selfinfo.dataport,
            selfinfo.ipaddress,
            (
                SELECT CASE 
                    WHEN rcapacity > (
                            SELECT system_capacity 
                            FROM selfinfo
                        ) 
                    THEN system_capacity
                    ELSE rcapacity
                    END
                FROM conn_info_receiving
            )  data_capacity
        FROM conn_info_receiving, selfinfo
    )
    INSERT INTO job_scheduler (jobdata, jtype, jstate, jsource, jdestination, jpriority, jobid, jparent_jobid)
    SELECT 
        create_message (
        
            uuid_data::text::bytea, '5'::text, 
            ( lpad(data_capacity::text, 11, ' ') || (lpad(dataport::text, 11, ' ')) || (lpad(ipaddress::text, 11, ' ')) )::bytea, 
            ''::bytea, system_name::text, destination::text, '5'::text, 147
        
        ), '5', 'S-5', system_name, destination, 5, uuid_data, jparent_jobid 
    FROM conn_info;


    UPDATE job_scheduler
    SET jstate = 'C'
    WHERE jstate = 'N-4'
    AND jtype = lpad('4', 5, ' ');


    WITH cte_sysinfo AS (
        
        SELECT encode(substr(js.jobdata, 115, 11), 'escape')::INTEGER as system_capacity, 
            encode(substr(js.jobdata, 126, 11), 'escape')::INTEGER as dataport
        FROM job_scheduler js 
        JOIN sysinfo si 
        ON (encode(substr(js.jobdata, 74, 5), 'escape')) = si.system_name
        AND (encode(substr(js.jobdata, 137, 11), 'escape'))::BIGINT = si.ipaddress
        WHERE jstate = 'N-4' 
        AND jtype = lpad('5', 5, ' ')
    )
    UPDATE sysinfo 
    SET (system_capacity, dataport) = (
        SELECT system_capacity, dataport
        FROM cte_sysinfo
    )
    WHERE system_capacity = 0 
    AND dataport = 0
    AND EXISTS (
            SELECT 1 
            FROM cte_sysinfo 
        );


    UPDATE job_scheduler
    SET jstate = 'C'
    WHERE jstate = 'N-4'
    AND jtype = lpad('5', 5, ' ');


    UPDATE job_scheduler 
    SET jstate = 'S-2' 
    WHERE jobdata IN (

            SELECT file_name::bytea 
            FROM files f 
            JOIN sysinfo si 
            ON jdestination = si.system_name 
            AND LENGTH(lo_get(f.file_data)) > (si.system_capacity-172)
            AND si.system_capacity != 0
        
        )
    AND jstate = 'S-1';


    UPDATE job_scheduler 
    SET jstate = 'S-3' 
    WHERE jobdata IN ( 
        
            SELECT file_name::bytea 
            FROM files 
            JOIN sysinfo si 
            ON jdestination = si.system_name 
            AND LENGTH(lo_get(file_data)) <= (si.system_capacity-172)
            AND si.system_capacity != 0
        )
    AND jstate = 'S-1';


    WITH single_job AS 
    (
        SELECT create_message( 
            
                gen_random_uuid()::text::bytea, 
                '1'::text, 
                ''::bytea, 
                lo_get(fd.file_data), 
                btrim(js.jsource, ' '), 
                btrim(js.jdestination, ' '), 
                js.jpriority,
                si.system_capacity
            
            ) mdata, jsource, js.jobid AS parent_jobid, js.jdestination, js.jpriority 
        FROM files fd 
        JOIN job_scheduler js 
        ON fd.file_name::bytea = js.jobdata
        JOIN sysinfo si 
        ON js.jdestination = si.system_name
        WHERE jstate = 'S-3'    
    )
    INSERT INTO job_scheduler (jobdata, jstate, jtype, jsource, jobid, jparent_jobid, jdestination, jpriority)
    SELECT mdata, 'S-4', '1', jsource, encode(substr(mdata, 33, 36), 'escape')::uuid, parent_jobid, jdestination, jpriority 
    FROM single_job; 


    WITH par_job AS (
            
        SELECT fd.file_data, js.jobid AS parent_jobid, length(lo_get(file_data)) AS datal, 
            js.jdestination, js.jsource, js.jpriority, si.system_capacity 
        FROM files fd 
        JOIN job_scheduler js 
        ON fd.file_name::bytea = js.jobdata
        JOIN sysinfo si 
        ON js.jdestination = si.system_name
        WHERE jstate = 'S-2'
    ),
    chunk_info AS (

        SELECT idx, gen_random_uuid()::text::bytea AS uuid_data, 
            parent_jobid, jdestination, jsource, jpriority, system_capacity,  
            lo_get(file_data, (idx*(system_capacity-172))::BIGINT, system_capacity::INTEGER - 172) chunk_data 
        FROM par_job, generate_series(0, ceil((datal)::decimal/(system_capacity - 172))-1) idx

    )
    INSERT INTO job_scheduler (jobdata, jstate, jtype, jsource, jobid, jparent_jobid, jdestination, jpriority)
    SELECT create_message( 
    
            uuid_data, '2'::text, 
            lpad(idx::text, 8, ' ')::bytea || parent_jobid::text::bytea || lpad(length(chunk_data)::text, 10, ' ')::bytea, 
            chunk_data, btrim(jsource, ' '), btrim(jdestination, ' '), jpriority::text, system_capacity

        ),'S-4', '2', jsource, encode(uuid_data, 'escape')::uuid, parent_jobid, jdestination, jpriority 
    FROM chunk_info;



    WITH cte_msginfo as(

        SELECT create_message(  
                
                gen_random_uuid()::text::bytea, '3'::text,
                js.jobid::text::bytea || 
                lpad(length((lo_get(file_data)))::text, 12, ' ')::bytea || 
                lpad((ceil(length(lo_get(file_data))::decimal/ (system_capacity -172)))::text, 10, ' ')::bytea,
                ''::bytea, 
                btrim(jsource, ' '), 
                btrim(jdestination, ' '), 
                jpriority::text,
                si.system_capacity

        ) as mdata, js.jobid, js.jdestination, js.jsource, js.jpriority, si.system_capacity
        FROM files fd 
        JOIN job_scheduler js 
        ON fd.file_name::bytea = js.jobdata
        JOIN sysinfo si 
        ON js.jdestination = si.system_name
        WHERE jstate = 'S-2'
    )
    INSERT INTO job_scheduler (jobdata, jstate, jtype, jsource, jobid, jparent_jobid, jdestination, jpriority)
    SELECT mdata, 'S-4', '3', jsource, encode(substr(mdata, 33, 36), 'escape')::uuid, jobid, jdestination, jpriority 
    FROM cte_msginfo; 



    UPDATE job_scheduler 
    SET jstate = 'S-2W' 
    WHERE jstate = 'S-2'
    AND jdestination IN (
            SELECT system_name
            FROM sysinfo
        );


    UPDATE job_scheduler 
    SET jstate = 'S-3W' 
    WHERE jstate = 'S-3'
    AND jdestination IN (
            SELECT system_name
            FROM sysinfo
        );


    INSERT INTO sending_conns (sfd, sipaddr, scstatus) 
    SELECT DISTINCT -1, si.ipaddress, 1  
    FROM job_scheduler js 
    JOIN sysinfo si
    ON si.system_name = jdestination
    WHERE js.jstate = 'S-4' 
    ON CONFLICT ON CONSTRAINT pk_sending_conns
    DO NOTHING;   


    INSERT INTO senders_comms (mdata1, mdata2, mtype)
    SELECT si.ipaddress, si.dataport, '1' 
    FROM sysinfo si
    JOIN sending_conns sc 
    ON si.ipaddress = sc.sipaddr
    JOIN job_scheduler js
    ON si.system_name = jdestination
    WHERE sc.scstatus = 1
    AND js.jstate = 'S-4'
    ON CONFLICT ON CONSTRAINT pk_senders_comms 
    DO NOTHING; 


    INSERT INTO senders_comms (mdata1, mdata2, mtype)
    SELECT sc.sipaddr, sc.sfd, '2'
    FROM sending_conns sc
    JOIN sysinfo si ON
    sc.sipaddr = si.ipaddress
    LEFT JOIN job_scheduler js
    ON si.system_name = js.jdestination  
    AND js.jstate != 'C'
    WHERE js.jdestination IS NULL
    AND sc.scstatus = 2
    ON CONFLICT ON CONSTRAINT pk_senders_comms 
    DO NOTHING;
   
    RETURN;

END;
$$
LANGUAGE 'plpgsql';



-- CREATE OR REPLACE FUNCTION GET_FILES_FROM_CHUNKS()
-- RETURNS VOID AS 
-- $$
-- DECLARE
--     lmsgid text;
--     newoid oid;
--     data_offset int;
--     nmsgdata bytea;
--     cur CURSOR FOR
--         WITH cte_msgdata AS (
--             SELECT substr(jobdata, 169, encode(substr(jobdata, 159, 10), 'escape')::integer) msgdata
--             FROM job_scheduler 
--             WHERE encode(substr(jobdata, 123, 36), 'escape') = lmsgid
--             AND jstate = 'N-4' 
--             AND encode(substr(jobdata, 69, 5), 'escape')::integer = 2
--             GROUP BY jobdata
--             ORDER BY encode(substr(jobdata, 115, 8), 'escape')::integer
--         )
--         SELECT msgdata
--         FROM cte_msgdata;
-- BEGIN
    
--     SELECT encode(substr(js1.jobdata, 115, 36), 'escape') msgid
--     INTO lmsgid
--     FROM job_scheduler js1
--     WHERE encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
--     AND js1.jstate = 'N-4'
--     AND encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
--         SELECT count(js2.jobdata) 
--         FROM job_scheduler js2
--         WHERE js2.jstate = 'N-4'
--         AND encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
--         AND encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
--     );

--     newoid := lo_creat(-1);  
--     INSERT INTO files (file_id, file_name, file_data) VALUES (gen_random_uuid(), 'new_file_' || gen_random_uuid()::text, newoid);

--     OPEN cur;
--     LOOP
--         FETCH cur INTO nmsgdata;
--         EXIT WHEN NOT FOUND;
        
--         data_offset := length(lo_get(newoid));
--         PERFORM lo_put(newoid, data_offset, nmsgdata);
--     END LOOP;
--     CLOSE cur;

-- END;
-- $$ LANGUAGE plpgsql;
-- select GET_FILES_FROM_CHUNKS();

-- create table temp_data_info(tmsgid uuid PRIMARY KEY, 
--                             datasize int NOT NULL, 
--                             chunk_count int NOT NULL, 
--                             source_name text NOT NULL);

-- create table temp_chunks(tmsgid uuid REFERENCES temp_data_info(tmsgid) ON UPDATE CASCADE ON DELETE CASCADE, 
--                         tdata bytea NOT NULL, 
--                         tchunk_number int NOT NULL);

-- with cte_arrnewmsg as (
--     select distinct encode(substr(js1.jobdata, 115, 36), 'escape')::uuid msgid,
--     encode(substr(js1.jobdata, 151, 12), 'escape')::int msg_size,
--     encode(substr(js1.jobdata, 163, 10), 'escape')::int chunk_count,
--     encode(substr(js1.jobdata, 74, 5), 'escape') source
--     from job_scheduler js1
--     where encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
--     and js1.jstate = 'N-4'
--     and encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
--         select count(js2.jobdata) 
--         from job_scheduler js2
--         where js2.jstate = 'N-4'
--         and encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
--         and encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
--     )
-- )
-- Insert into temp_data_info select msgid, msg_size, chunk_count, source from cte_arrnewmsg;


-- with cte_arrnewmsg as (
--     select distinct encode(substr(js1.jobdata, 115, 36), 'escape') msgid
--     from job_scheduler js1
--     where encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
--     and js1.jstate = 'N-4'
--     and encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
--         select count(js2.jobdata) 
--         from job_scheduler js2
--         where js2.jstate = 'N-4'
--         and encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
--         and encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
--     )
-- ),
-- cte_msgdata as (
--     select substr(js.jobdata, 169,  encode(substr(js.jobdata, 159, 10), 'escape')::integer) msgdata,
--     cte_arrnewmsg.msgid,
--     encode(substr(js.jobdata, 115, 8), 'escape')::integer chunk_num 
--     from job_scheduler js
--     join cte_arrnewmsg 
--     on encode(substr(js.jobdata, 123, 36), 'escape') = cte_arrnewmsg.msgid
--     where js.jstate = 'N-4' 
--     and encode(substr(js.jobdata, 69, 5), 'escape')::integer = 2
--     GROUP by js.jobdata, cte_arrnewmsg.msgid
--     order by encode(substr(js.jobdata, 115, 8), 'escape')::integer
-- )
-- INSERT INTO temp_chunks 
-- SELECT msgid::uuid, msgdata, chunk_num  
-- FROM cte_msgdata;


-- with cte_arrnewmsg as (
--     select distinct encode(substr(js1.jobdata, 115, 36), 'escape') msgid
--     from job_scheduler js1
--     where encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
--     and js1.jstate = 'N-4'
--     and encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
--         select count(js2.jobdata) 
--         from job_scheduler js2
--         where js2.jstate = 'N-4'
--         and encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
--         and encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
--     )
-- )
-- update job_scheduler 
-- set jstate = 'C'
-- where encode(substr(jobdata, 123, 36), 'escape') = (select msgid from cte_arrnewmsg)
-- or encode(substr(jobdata, 115, 36), 'escape') = (select msgid from cte_arrnewmsg);





