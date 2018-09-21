/*
This function determines which connection string to use for sql queries.
It's intended to be used whenever a function needs to be portable,
ie, you need a function to make few assumptions about dsn and other
machine specific settings.

Usage:
create_connection_string
local conn_string = r(conn_string)

odbc, load ("select foo from baz") `conn_string'

Arguments: None required.
Optional arguments are server, database, user, password
Returns: r(conn_string)

*/

cap program drop create_connection_string
program create_connection_string, rclass
   syntax, [server(string) database(string) ///
     user(string) password(string)]

   // set unix odbc manager
   if c(os) == "Unix" set odbcmgr unixodbc
	 
   // set dUSERt arguments, if not specified
   if "`server'" == ""   local server = "strServer"
   if "`database'" == "" local database = "strDatabase"
   if "`user'" == ""     local user = "strUser"
   if "`password'" == "" local password = "strPassword"

   // assign drivers to attempt to connect with
   local driver1 = `"strConnection"'
   local driver2 = `"strConnection"'
   
   // Loop through all connection strings and see if any work
   local passes = 0
   foreach driver in "`driver1'" "`driver2'" {

      local conn_string = `"conn("DRIVER={`driver'};"' ///
        + `"SERVER=`server'strServer;DATABASE=`database';"' ///
        + `"UID=`user';PWD=`password';")"'

      cap odbc exec("select 1"), `conn_string'

      if !_rc {
         return local conn_string "`conn_string'"
		 local passes = 1
         }
	  
      }

   if `passes' != 1 {
      di as error "Invalid connection arguments supplied"
      error(999)
      }
   
end
