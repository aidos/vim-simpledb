--- Per-buffer persistent connection manager with async connect/query.

local pq = require("simpledb.libpq")
local uv = vim.uv or vim.loop

local M = {}

--- Map of bufnr -> connection state table.
--- Each entry: { conn = PGconn*, poll_handle = uv_poll_t|nil, state = string, queue = {} }
--- States: "disconnected", "connecting", "ready", "busy", "error"
local connections = {}

--- Get the connection state for a buffer, or nil.
---@param bufnr number
---@return table|nil
function M.get(bufnr)
  return connections[bufnr]
end

--- Parse line 1 of a buffer for the connection string.
--- Strips the leading "-- " SQL comment prefix.
---@param bufnr number
---@return string|nil conninfo, string|nil error
function M.parse_conninfo(bufnr)
  local line = vim.api.nvim_buf_get_lines(bufnr, 0, 1, false)[1]
  if not line then
    return nil, "buffer is empty"
  end
  local conninfo = line:match("^%s*%-%-%s*(.*)")
  if not conninfo or conninfo:match("^%s*$") then
    return nil, "line 1 must be an SQL comment with connection info, e.g.: -- host=localhost dbname=mydb"
  end
  return conninfo
end

--- Stop and close the uv poll handle for a connection state.
---@param state table connection state
local function stop_poll(state)
  if state.poll_handle then
    if not state.poll_handle:is_closing() then
      state.poll_handle:stop()
      state.poll_handle:close()
    end
    state.poll_handle = nil
  end
end

--- Destroy a connection, releasing all resources.
---@param bufnr number
function M.disconnect(bufnr)
  local state = connections[bufnr]
  if not state then
    return
  end
  stop_poll(state)
  if state.conn then
    pq.finish(state.conn)
    state.conn = nil
  end
  state.state = "disconnected"
  connections[bufnr] = nil
end

--- Finish async connect: set nonblocking, transition to "ready", flush queue.
---@param state table connection state
---@param bufnr number
local function on_connect_success(state, bufnr)
  pq.set_nonblocking(state.conn)
  state.state = "ready"
  local info = pq.conn_info(state.conn)
  vim.schedule(function()
    vim.api.nvim_echo(
      { { string.format("simpledb: connected to %s@%s/%s", info.user, info.host, info.db), "Normal" } },
      false,
      {}
    )
  end)
  -- Flush any queued queries
  if #state.queue > 0 then
    local next_item = table.remove(state.queue, 1)
    vim.schedule(function()
      M.send_query(bufnr, next_item.sql, next_item.callback)
    end)
  end
end

--- Start an async connection for a buffer.
---@param bufnr number
---@param conninfo string
---@param callback fun(err: string|nil) called when connect completes or fails
function M.connect_async(bufnr, conninfo, callback)
  -- Clean up any existing connection
  M.disconnect(bufnr)

  local conn, err = pq.connect_start(conninfo)
  if not conn then
    callback(err)
    return
  end

  local state = {
    conn = conn,
    poll_handle = nil,
    state = "connecting",
    queue = {},
    conninfo = conninfo,
  }
  connections[bufnr] = state

  local fd = pq.socket(conn)
  if fd < 0 then
    M.disconnect(bufnr)
    callback("could not get connection socket")
    return
  end

  local poll_handle = uv.new_poll(fd)
  state.poll_handle = poll_handle

  local function poll_connect()
    local poll_status = pq.connect_poll(conn)

    if poll_status == pq.PGRES_POLLING_OK then
      stop_poll(state)
      on_connect_success(state, bufnr)
      vim.schedule(function()
        callback(nil)
      end)
    elseif poll_status == pq.PGRES_POLLING_FAILED then
      local errmsg = pq.error_message(conn)
      M.disconnect(bufnr)
      vim.schedule(function()
        callback(errmsg)
      end)
    elseif poll_status == pq.PGRES_POLLING_READING then
      poll_handle:start("r", function(poll_err)
        if poll_err then
          M.disconnect(bufnr)
          vim.schedule(function()
            callback("poll error: " .. poll_err)
          end)
          return
        end
        poll_connect()
      end)
    elseif poll_status == pq.PGRES_POLLING_WRITING then
      poll_handle:start("w", function(poll_err)
        if poll_err then
          M.disconnect(bufnr)
          vim.schedule(function()
            callback("poll error: " .. poll_err)
          end)
          return
        end
        poll_connect()
      end)
    end
  end

  -- Kick off the polling loop
  poll_connect()
end

--- Parse a single PGresult into a structured entry.
---@param res ffi.cdata* PGresult pointer
---@return table entry
local function parse_result(res)
  local status = pq.result_status(res)
  local entry = { status = status }

  if status == pq.PGRES_TUPLES_OK or status == pq.PGRES_SINGLE_TUPLE then
    -- SELECT-like: extract columns and rows
    local ncols = pq.nfields(res)
    local nrows = pq.ntuples(res)
    local columns = {}
    for c = 0, ncols - 1 do
      columns[c + 1] = pq.fname(res, c)
    end
    local rows = {}
    for r = 0, nrows - 1 do
      local row = {}
      for c = 0, ncols - 1 do
        if pq.getisnull(res, r, c) then
          row[c + 1] = nil -- NULL
        else
          row[c + 1] = pq.getvalue(res, r, c)
        end
      end
      rows[r + 1] = row
    end
    entry.columns = columns
    entry.rows = rows
    entry.nrows = nrows
  elseif status == pq.PGRES_COMMAND_OK then
    -- INSERT/UPDATE/DELETE/CREATE etc.
    entry.cmd_status = pq.cmd_status(res)
    entry.cmd_tuples = pq.cmd_tuples(res)
  elseif status == pq.PGRES_FATAL_ERROR then
    entry.error = pq.result_error_message(res)
  elseif status == pq.PGRES_EMPTY_QUERY then
    entry.error = "empty query"
  else
    entry.error = pq.result_error_message(res)
  end

  return entry
end

--- Collect all results from a completed async query.
--- PQgetResult must be called until it returns NULL to clear the pipeline.
--- Handles COPY OUT by draining copy data, and COPY IN by cancelling.
---@param state table connection state
---@return table results list of parsed result tables
local function collect_results(state)
  local results = {}
  while true do
    local res = pq.get_result(state.conn)
    if res == nil then
      break
    end

    local status = pq.result_status(res)

    if status == pq.PGRES_COPY_OUT then
      -- Signal that we need async COPY OUT draining.
      -- Store partial results collected so far.
      state._partial_results = results
      state._copy_chunks = {}
      return nil -- signal: need COPY OUT drain
    elseif status == pq.PGRES_COPY_IN then
      -- We don't support COPY FROM STDIN; cancel it
      pq.put_copy_end(state.conn, "COPY FROM STDIN not supported by simpledb")
      results[#results + 1] = {
        status = pq.PGRES_FATAL_ERROR,
        error = "COPY FROM STDIN is not supported by simpledb",
      }
    else
      results[#results + 1] = parse_result(res)
    end
    -- res will be PQclear'd by ffi.gc when it goes out of scope
  end
  return results
end

--- Async drain of COPY OUT data. Polls for readable, consumes input, then
--- reads copy data until done or error.
---@param state table connection state
---@param on_done fun() called when COPY OUT is fully drained
local function drain_copy_out(state, on_done)
  local fd = pq.socket(state.conn)
  if fd < 0 then
    on_done()
    return
  end

  stop_poll(state)
  local poll_handle = uv.new_poll(fd)
  state.poll_handle = poll_handle

  local function poll_copy()
    poll_handle:start("r", function(poll_err)
      if poll_err then
        stop_poll(state)
        on_done()
        return
      end

      if not pq.consume_input(state.conn) then
        stop_poll(state)
        on_done()
        return
      end

      -- Try to read available copy data
      state._copy_chunks = state._copy_chunks or {}
      while true do
        local len, data = pq.get_copy_data(state.conn, true)
        if len > 0 and data then
          state._copy_chunks[#state._copy_chunks + 1] = data
        elseif len == 0 then
          -- Need more data, poll again
          poll_copy()
          return
        elseif len == -1 then
          -- COPY done
          stop_poll(state)
          -- Build the result entry from accumulated chunks
          local copy_chunks = state._copy_chunks
          state._copy_chunks = nil
          state._copy_in_progress = nil
          if #copy_chunks > 0 then
            local copy_text = table.concat(copy_chunks)
            local rows = {}
            for line in copy_text:gmatch("([^\n]*)\n?") do
              if line ~= "" then
                rows[#rows + 1] = { line }
              end
            end
            local partial = state._partial_results or {}
            partial[#partial + 1] = {
              status = pq.PGRES_TUPLES_OK,
              columns = { "COPY" },
              rows = rows,
              nrows = #rows,
            }
            state._partial_results = partial
          end
          on_done()
          return
        else -- len == -2, error
          stop_poll(state)
          state._copy_chunks = nil
          state._copy_in_progress = nil
          local partial = state._partial_results or {}
          partial[#partial + 1] = {
            status = pq.PGRES_FATAL_ERROR,
            error = pq.error_message(state.conn),
          }
          state._partial_results = partial
          on_done()
          return
        end
      end
    end)
  end

  poll_copy()
end

--- Send a query on an established connection. Fully async.
---@param bufnr number
---@param sql string
---@param callback fun(err: string|nil, results: table|nil, elapsed_ms: number|nil)
function M.send_query(bufnr, sql, callback)
  local state = connections[bufnr]
  if not state then
    callback("no connection for this buffer")
    return
  end

  if state.state == "connecting" then
    -- Queue the query; it will be sent after connect completes
    table.insert(state.queue, { sql = sql, callback = callback })
    return
  end

  if state.state == "busy" then
    -- Queue the query; it will be sent after current query completes
    table.insert(state.queue, { sql = sql, callback = callback })
    return
  end

  if state.state ~= "ready" then
    callback("connection is in state: " .. state.state)
    return
  end

  -- Check that the connection is still alive
  if pq.status(state.conn) ~= 0 then
    -- CONNECTION_OK == 0
    callback("connection lost (status: " .. tostring(pq.status(state.conn)) .. ")")
    state.state = "error"
    return
  end

  local ok, err = pq.send_query(state.conn, sql)
  if not ok then
    callback(err)
    return
  end

  state.state = "busy"
  local start_time = uv.hrtime()

  local fd = pq.socket(state.conn)
  if fd < 0 then
    state.state = "error"
    callback("could not get connection socket")
    return
  end

  -- Ensure we don't have a stale poll handle
  stop_poll(state)

  local poll_handle = uv.new_poll(fd)
  state.poll_handle = poll_handle

  local function poll_result()
    poll_handle:start("r", function(poll_err)
      if poll_err then
        stop_poll(state)
        state.state = "ready"
        vim.schedule(function()
          callback("poll error: " .. poll_err)
        end)
        return
      end

      if not pq.consume_input(state.conn) then
        stop_poll(state)
        state.state = "error"
        vim.schedule(function()
          callback("error consuming input: " .. pq.error_message(state.conn))
        end)
        return
      end

      if pq.is_busy(state.conn) then
        -- Not done yet, keep polling
        poll_result()
        return
      end

      -- Try to collect results (may return nil if COPY OUT needs more data)
      stop_poll(state)
      local results = collect_results(state)
      if results == nil then
        -- COPY OUT in progress, need to read more data from socket
        drain_copy_out(state, function()
          -- After COPY drain, collect remaining results (final COMMAND_OK etc.)
          local all_results = state._partial_results or {}
          state._partial_results = nil
          -- Continue collecting any remaining PQgetResult entries
          while true do
            local res2 = pq.get_result(state.conn)
            if res2 == nil then break end
            local s = pq.result_status(res2)
            if s == pq.PGRES_COMMAND_OK then
              all_results[#all_results + 1] = parse_result(res2)
            end
            -- skip other statuses (already handled)
          end
          local elapsed_ms2 = (uv.hrtime() - start_time) / 1e6
          state.state = "ready"
          vim.schedule(function()
            callback(nil, all_results, elapsed_ms2)
          end)
          if #state.queue > 0 then
            local next_item = table.remove(state.queue, 1)
            vim.schedule(function()
              M.send_query(bufnr, next_item.sql, next_item.callback)
            end)
          end
        end)
        return
      end
      local elapsed_ms = (uv.hrtime() - start_time) / 1e6
      state.state = "ready"

      vim.schedule(function()
        callback(nil, results, elapsed_ms)
      end)

      -- Process next queued query if any
      if #state.queue > 0 then
        local next_item = table.remove(state.queue, 1)
        vim.schedule(function()
          M.send_query(bufnr, next_item.sql, next_item.callback)
        end)
      end
    end)
  end

  -- Start polling for results
  poll_result()
end

--- Get or create a connection for a buffer, then call the callback.
--- If already connected, calls back immediately. If not, starts async connect.
---@param bufnr number
---@param callback fun(err: string|nil)
function M.ensure_connected(bufnr, callback)
  local state = connections[bufnr]
  if state and (state.state == "ready" or state.state == "busy") then
    callback(nil)
    return
  end

  local conninfo, err = M.parse_conninfo(bufnr)
  if not conninfo then
    callback(err)
    return
  end

  vim.api.nvim_echo({ { "simpledb: connecting...", "Normal" } }, false, {})

  M.connect_async(bufnr, conninfo, callback)
end

--- Return status info for the current buffer's connection.
---@param bufnr number
---@return table {state=string, info=table|nil, queue_depth=number}
function M.status(bufnr)
  local state = connections[bufnr]
  if not state then
    return { state = "disconnected", queue_depth = 0 }
  end
  local info = nil
  if state.conn and state.state ~= "disconnected" then
    info = pq.conn_info(state.conn)
  end
  return {
    state = state.state,
    info = info,
    queue_depth = #state.queue,
  }
end

--- Set up autocmd to disconnect when a buffer is deleted.
local augroup = vim.api.nvim_create_augroup("simpledb_connections", { clear = true })
vim.api.nvim_create_autocmd({ "BufDelete", "BufWipeout" }, {
  group = augroup,
  callback = function(args)
    local bufnr = args.buf
    if connections[bufnr] then
      M.disconnect(bufnr)
    end
  end,
})

--- Disconnect all connections (for plugin cleanup / VimLeave).
function M.disconnect_all()
  for bufnr, _ in pairs(connections) do
    M.disconnect(bufnr)
  end
end

vim.api.nvim_create_autocmd("VimLeavePre", {
  group = augroup,
  callback = function()
    M.disconnect_all()
  end,
})

return M
