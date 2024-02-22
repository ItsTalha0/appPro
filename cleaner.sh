echo -n "enter db:-"
#read DBNAME
psql -d pix -c "\i ~/prj/msgapp/db/build_schema.sql"
psql -d pix1 -c "\i ~/prj/msgapp/db/build_schema1.sql"

echo -n "db Reset complete"
echo  "removing locks"

ipcrm shm `ipcs | awk '$1 != "0x00000000" && $3 == "pix"  {printf "%d ", $2}'`
