--- FFI bindings to libpq for persistent PostgreSQL connections.
--- Provides thin Lua wrappers around the essential libpq functions.

local ffi = require("ffi")

ffi.cdef([[
  /* Opaque types */
  typedef struct pg_conn PGconn;
  typedef struct pg_result PGresult;
  typedef unsigned int Oid;

  /* Connection status */
  typedef enum {
    CONNECTION_OK,
    CONNECTION_BAD,
    CONNECTION_STARTED,
    CONNECTION_MADE,
    CONNECTION_AWAITING_RESPONSE,
    CONNECTION_AUTH_OK,
    CONNECTION_SETENV,
    CONNECTION_SSL_STARTUP,
    CONNECTION_NEEDED,
    CONNECTION_CHECK_WRITABLE,
    CONNECTION_CONSUME,
    CONNECTION_GSS_STARTUP,
    CONNECTION_CHECK_TARGET,
    CONNECTION_CHECK_STANDBY
  } ConnStatusType;

  /* Polling status for async connect */
  typedef enum {
    PGRES_POLLING_FAILED = 0,
    PGRES_POLLING_READING,
    PGRES_POLLING_WRITING,
    PGRES_POLLING_OK,
    PGRES_POLLING_ACTIVE
  } PostgresPollingStatusType;

  /* Exec status for results */
  typedef enum {
    PGRES_EMPTY_QUERY = 0,
    PGRES_COMMAND_OK,
    PGRES_TUPLES_OK,
    PGRES_COPY_OUT,
    PGRES_COPY_IN,
    PGRES_BAD_RESPONSE,
    PGRES_NONFATAL_ERROR,
    PGRES_FATAL_ERROR,
    PGRES_COPY_BOTH,
    PGRES_SINGLE_TUPLE,
    PGRES_PIPELINE_SYNC,
    PGRES_PIPELINE_ABORTED
  } ExecStatusType;

  /* Synchronous connection */
  PGconn *PQconnectdb(const char *conninfo);

  /* Async connection */
  PGconn *PQconnectStart(const char *conninfo);
  PostgresPollingStatusType PQconnectPoll(PGconn *conn);

  /* Connection info */
  ConnStatusType PQstatus(const PGconn *conn);
  char *PQerrorMessage(const PGconn *conn);
  int PQsocket(const PGconn *conn);
  char *PQdb(const PGconn *conn);
  char *PQhost(const PGconn *conn);
  char *PQuser(const PGconn *conn);

  /* Connection control */
  void PQfinish(PGconn *conn);
  int PQsetnonblocking(PGconn *conn, int arg);
  void PQreset(PGconn *conn);
  int PQresetStart(PGconn *conn);
  PostgresPollingStatusType PQresetPoll(PGconn *conn);

  /* Async query execution */
  int PQsendQuery(PGconn *conn, const char *query);
  int PQconsumeInput(PGconn *conn);
  int PQisBusy(PGconn *conn);
  PGresult *PQgetResult(PGconn *conn);
  int PQflush(PGconn *conn);

  /* Result inspection */
  ExecStatusType PQresultStatus(const PGresult *res);
  char *PQresultErrorMessage(const PGresult *res);
  char *PQresultErrorField(const PGresult *res, int fieldcode);
  int PQntuples(const PGresult *res);
  int PQnfields(const PGresult *res);
  char *PQfname(const PGresult *res, int field_num);
  char *PQgetvalue(const PGresult *res, int tup_num, int field_num);
  int PQgetisnull(const PGresult *res, int tup_num, int field_num);
  int PQfsize(const PGresult *res, int field_num);
  Oid PQftype(const PGresult *res, int field_num);
  char *PQcmdTuples(const PGresult *res);
  char *PQcmdStatus(const PGresult *res);

  /* Result memory */
  void PQclear(PGresult *res);
]])

local M = {}

-- Load libpq. Try common library names.
local ok, lib = pcall(ffi.load, "pq")
if not ok then
  ok, lib = pcall(ffi.load, "libpq")
end
if not ok then
  ok, lib = pcall(ffi.load, "libpq.so.5")
end
if not ok then
  error(
    "simpledb: could not load libpq. "
      .. "Ensure libpq is installed (e.g. `apt install libpq5` or `brew install libpq`).\n"
      .. "Original error: "
      .. lib
  )
end

M.lib = lib

--- Convert a C string pointer to a Lua string, returning nil for NULL.
---@param cstr ffi.cdata*
---@return string|nil
local function cstr(ptr)
  if ptr == nil then
    return nil
  end
  return ffi.string(ptr)
end

M.cstr = cstr

-- Enum values as numbers for comparison
M.CONNECTION_OK = tonumber(ffi.cast("int", ffi.C and 0 or 0))
M.CONNECTION_BAD = 1

M.PGRES_POLLING_FAILED = 0
M.PGRES_POLLING_READING = 1
M.PGRES_POLLING_WRITING = 2
M.PGRES_POLLING_OK = 3

M.PGRES_EMPTY_QUERY = 0
M.PGRES_COMMAND_OK = 1
M.PGRES_TUPLES_OK = 2
M.PGRES_COPY_OUT = 3
M.PGRES_COPY_IN = 4
M.PGRES_BAD_RESPONSE = 5
M.PGRES_NONFATAL_ERROR = 6
M.PGRES_FATAL_ERROR = 7
M.PGRES_SINGLE_TUPLE = 9

--- Start an asynchronous connection.
---@param conninfo string libpq connection string
---@return ffi.cdata* PGconn pointer (or nil + error message)
function M.connect_start(conninfo)
  local conn = lib.PQconnectStart(conninfo)
  if conn == nil then
    return nil, "PQconnectStart returned NULL"
  end
  if tonumber(lib.PQstatus(conn)) == M.CONNECTION_BAD then
    local err = cstr(lib.PQerrorMessage(conn))
    lib.PQfinish(conn)
    return nil, err or "connection failed"
  end
  return conn
end

--- Poll an async connection in progress.
---@param conn ffi.cdata* PGconn pointer
---@return number PostgresPollingStatusType as integer
function M.connect_poll(conn)
  return tonumber(lib.PQconnectPoll(conn))
end

--- Get connection status.
---@param conn ffi.cdata* PGconn pointer
---@return number ConnStatusType as integer
function M.status(conn)
  return tonumber(lib.PQstatus(conn))
end

--- Get error message for the connection.
---@param conn ffi.cdata* PGconn pointer
---@return string
function M.error_message(conn)
  return cstr(lib.PQerrorMessage(conn)) or ""
end

--- Get the socket file descriptor.
---@param conn ffi.cdata* PGconn pointer
---@return number
function M.socket(conn)
  return tonumber(lib.PQsocket(conn))
end

--- Set connection to non-blocking mode.
---@param conn ffi.cdata* PGconn pointer
---@return boolean success
function M.set_nonblocking(conn)
  return lib.PQsetnonblocking(conn, 1) == 0
end

--- Send a query asynchronously.
---@param conn ffi.cdata* PGconn pointer
---@param query string SQL query
---@return boolean success
---@return string|nil error message on failure
function M.send_query(conn, query)
  local ret = lib.PQsendQuery(conn, query)
  if ret == 0 then
    return false, cstr(lib.PQerrorMessage(conn))
  end
  return true
end

--- Consume available input from the server.
---@param conn ffi.cdata* PGconn pointer
---@return boolean success
function M.consume_input(conn)
  return lib.PQconsumeInput(conn) == 1
end

--- Check if the connection is busy (waiting for more data).
---@param conn ffi.cdata* PGconn pointer
---@return boolean busy
function M.is_busy(conn)
  return lib.PQisBusy(conn) == 1
end

--- Flush any queued output data to the server.
---@param conn ffi.cdata* PGconn pointer
---@return number 0=done, 1=more to flush, -1=error
function M.flush(conn)
  return tonumber(lib.PQflush(conn))
end

--- Get the next result from an async query.
--- Returns nil when no more results.
---@param conn ffi.cdata* PGconn pointer
---@return ffi.cdata*|nil PGresult pointer
function M.get_result(conn)
  local res = lib.PQgetResult(conn)
  if res == nil then
    return nil
  end
  return ffi.gc(res, lib.PQclear)
end

--- Get the result status code.
---@param res ffi.cdata* PGresult pointer
---@return number ExecStatusType as integer
function M.result_status(res)
  return tonumber(lib.PQresultStatus(res))
end

--- Get the error message from a result.
---@param res ffi.cdata* PGresult pointer
---@return string
function M.result_error_message(res)
  return cstr(lib.PQresultErrorMessage(res)) or ""
end

--- Get the command status string (e.g. "SELECT 5", "INSERT 0 1").
---@param res ffi.cdata* PGresult pointer
---@return string
function M.cmd_status(res)
  return cstr(lib.PQcmdStatus(res)) or ""
end

--- Get the number of rows affected by the command.
---@param res ffi.cdata* PGresult pointer
---@return string number as string (empty string if not applicable)
function M.cmd_tuples(res)
  return cstr(lib.PQcmdTuples(res)) or ""
end

--- Get the number of rows in the result.
---@param res ffi.cdata* PGresult pointer
---@return number
function M.ntuples(res)
  return tonumber(lib.PQntuples(res))
end

--- Get the number of columns in the result.
---@param res ffi.cdata* PGresult pointer
---@return number
function M.nfields(res)
  return tonumber(lib.PQnfields(res))
end

--- Get the name of a column.
---@param res ffi.cdata* PGresult pointer
---@param col number 0-indexed column number
---@return string
function M.fname(res, col)
  return cstr(lib.PQfname(res, col)) or ""
end

--- Get the value of a field.
---@param res ffi.cdata* PGresult pointer
---@param row number 0-indexed row number
---@param col number 0-indexed column number
---@return string
function M.getvalue(res, row, col)
  return cstr(lib.PQgetvalue(res, row, col)) or ""
end

--- Check if a field is NULL.
---@param res ffi.cdata* PGresult pointer
---@param row number 0-indexed row number
---@param col number 0-indexed column number
---@return boolean
function M.getisnull(res, row, col)
  return lib.PQgetisnull(res, row, col) == 1
end

--- Close a connection and free resources.
---@param conn ffi.cdata* PGconn pointer
function M.finish(conn)
  if conn ~= nil then
    lib.PQfinish(conn)
  end
end

--- Get connection metadata.
---@param conn ffi.cdata* PGconn pointer
---@return table {db=string, host=string, user=string}
function M.conn_info(conn)
  return {
    db = cstr(lib.PQdb(conn)) or "",
    host = cstr(lib.PQhost(conn)) or "",
    user = cstr(lib.PQuser(conn)) or "",
  }
end

--- Start an async connection reset (reconnect).
---@param conn ffi.cdata* PGconn pointer
---@return boolean success
function M.reset_start(conn)
  return lib.PQresetStart(conn) == 1
end

--- Poll an async connection reset.
---@param conn ffi.cdata* PGconn pointer
---@return number PostgresPollingStatusType as integer
function M.reset_poll(conn)
  return tonumber(lib.PQresetPoll(conn))
end

return M
