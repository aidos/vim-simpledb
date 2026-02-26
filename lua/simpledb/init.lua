--- simpledb: persistent PostgreSQL connections for Neovim.
--- Public API module.

local conn_manager = require("simpledb.connection")
local query_mod = require("simpledb.query")
local display = require("simpledb.display")

local M = {}

--- Execute SQL from a line range in the current buffer.
--- Lazy-connects on first use, then sends the query async.
---@param firstline number 1-indexed first line of the query
---@param lastline number 1-indexed last line of the query
function M.execute(firstline, lastline)
  local bufnr = vim.api.nvim_get_current_buf()

  -- Extract the query from the selected lines
  local sql = query_mod.get_query(bufnr, firstline, lastline)
  if sql:match("^%s*$") then
    vim.api.nvim_echo({ { "simpledb: no query to execute", "WarningMsg" } }, false, {})
    return
  end

  -- Apply wrapper template if present
  sql = query_mod.apply_wrapper(bufnr, sql)

  -- Show executing status
  vim.api.nvim_echo({ { "simpledb: executing query...", "Normal" } }, false, {})

  -- Ensure we have a connection, then send the query
  conn_manager.ensure_connected(bufnr, function(err)
    if err then
      display.show_error(bufnr, err)
      vim.api.nvim_echo({ { "simpledb: connection failed: " .. err, "ErrorMsg" } }, false, {})
      return
    end

    conn_manager.send_query(bufnr, sql, function(query_err, results, elapsed_ms)
      if query_err then
        display.show_error(bufnr, query_err)
        vim.api.nvim_echo({ { "simpledb: query error", "ErrorMsg" } }, false, {})
        return
      end

      local show_timing = vim.g.simpledb_show_timing
      if show_timing == 0 then
        elapsed_ms = nil
      end

      local lines = display.format_results(results, elapsed_ms)
      display.show(bufnr, lines)

      -- Clear the "executing" message
      vim.api.nvim_echo({ { "" } }, false, {})
    end)
  end)
end

--- Disconnect the current buffer's connection.
function M.disconnect()
  local bufnr = vim.api.nvim_get_current_buf()
  local state = conn_manager.get(bufnr)
  if not state then
    vim.api.nvim_echo({ { "simpledb: no active connection", "WarningMsg" } }, false, {})
    return
  end
  conn_manager.disconnect(bufnr)
  vim.api.nvim_echo({ { "simpledb: disconnected", "Normal" } }, false, {})
end

--- Show connection status for the current buffer.
function M.status()
  local bufnr = vim.api.nvim_get_current_buf()
  local info = conn_manager.status(bufnr)

  if info.state == "disconnected" then
    vim.api.nvim_echo({ { "simpledb: not connected", "Normal" } }, false, {})
    return
  end

  local msg = "simpledb: " .. info.state
  if info.info then
    msg = msg .. string.format(" (%s@%s/%s)", info.info.user, info.info.host, info.info.db)
  end
  if info.queue_depth > 0 then
    msg = msg .. string.format(" [%d queued]", info.queue_depth)
  end
  vim.api.nvim_echo({ { msg, "Normal" } }, false, {})
end

--- Reconnect the current buffer (disconnect + lazy reconnect on next query).
function M.reconnect()
  local bufnr = vim.api.nvim_get_current_buf()
  conn_manager.disconnect(bufnr)
  vim.api.nvim_echo({ { "simpledb: reconnecting...", "Normal" } }, false, {})
  conn_manager.ensure_connected(bufnr, function(err)
    if err then
      vim.api.nvim_echo({ { "simpledb: reconnect failed: " .. err, "ErrorMsg" } }, false, {})
    end
  end)
end

return M
