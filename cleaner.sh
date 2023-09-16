echo -n "enter db:-"
#read DBNAME
psql -d pix -c "\i ~/prj/msgapp/db/build_schema.sql"
psql -d pix1 -c "\i ~/prj/msgapp/db/build_schema1.sql"

echo -n "db Reset complete"
echo -n "removing locks"

rm /dev/shm/sem.*
