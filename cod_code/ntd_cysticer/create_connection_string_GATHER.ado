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
DUSERts are modeling-cod-db, shared and our readonly user/pw
Returns: r(conn_string)

*/

cap program drop create_connection_string
program create_connection_string, rclass
   syntax, [server(string) database(string) ///
     user(string) password(string)]

   // set unix odbc manager
   if c(os) == "Unix" {
        set odbcmgr unixodbc
        local j = "FILEPATH"
        }
   else {
        local j = "FILEPATH"
   }

   preserve

   local DEFAULT_CREDENTIALS = "FILEPATH/dUSERt.csv"
	 
   // set dUSERt arguments, if not specified
   if "`server'" == ""   local server = "modeling-cod-db"
   if "`database'" == "" local database = "shared"
   if "`user'" == "" & "`password'" == "" {
        import delimited using "`DEFAULT_CREDENTIALS'", clear varnames(1)
        qui levelsof user, local(user) cl
        qui levelsof pw, local(password) cl
   }

   // assign drivers to attempt to connect with
   local driver1 = `"MySQL ODBC 5.2 Unicode Driver"'
   local driver2 = `"MySQL ODBC 5.3 Unicode Driver"'
   
   // Loop through all connection strings and see if any work
   local passes = 0
   foreach driver in "`driver1'" "`driver2'" {

      local conn_string = `"conn("DRIVER={`driver'};"' ///
        + `"SERVER=`server'.ADDRESS;DATABASE=`database';"' ///
        + `"UID=`user';PWD=`password';")"'

      cap odbc exec("select 1"), `conn_string'

      if !_rc {
         return local conn_string "`conn_string'"
		 local passes = 1
         }
	  
      }

   restore

   if `passes' != 1 {
      di as error "Invalid connection arguments supplied"
      error(999)
      }
   
end
