--- Scratch buffer result display in a bottom split.

local M = {}

--- Map of source bufnr -> result bufnr.
local result_buffers = {}

--- Left-pad a string to a given width with spaces.
--- Works for any width (unlike string.format which caps at 99).
---@param s string
---@param width number
---@return string
local function pad_right(s, width)
  if #s >= width then
    return s
  end
  return s .. string.rep(" ", width - #s)
end

--- Format a result set as aligned text lines (psql-style table).
---@param result table { columns=string[], rows=string[][], nrows=number }
---@return string[] lines
local function format_tuples(result)
  local columns = result.columns
  local rows = result.rows
  local ncols = #columns

  -- Compute column widths (minimum = header width)
  local widths = {}
  for c = 1, ncols do
    widths[c] = #columns[c]
  end
  for _, row in ipairs(rows) do
    for c = 1, ncols do
      local val = row[c]
      local display = val == nil and "NULL" or val
      if #display > widths[c] then
        widths[c] = #display
      end
    end
  end

  local lines = {}

  -- Header row
  local header_parts = {}
  for c = 1, ncols do
    header_parts[c] = " " .. pad_right(columns[c], widths[c]) .. " "
  end
  lines[#lines + 1] = table.concat(header_parts, "|")

  -- Separator
  local sep_parts = {}
  for c = 1, ncols do
    sep_parts[c] = string.rep("-", widths[c] + 2)
  end
  lines[#lines + 1] = table.concat(sep_parts, "+")

  -- Data rows
  for _, row in ipairs(rows) do
    local parts = {}
    for c = 1, ncols do
      local val = row[c]
      local display = val == nil and "NULL" or val
      parts[c] = " " .. pad_right(display, widths[c]) .. " "
    end
    lines[#lines + 1] = table.concat(parts, "|")
  end

  -- Row count footer
  local nrows = result.nrows or #rows
  lines[#lines + 1] = string.format("(%d row%s)", nrows, nrows == 1 and "" or "s")

  return lines
end

--- Format a list of result sets into display lines.
---@param results table[] list of result tables from connection.collect_results
---@param elapsed_ms number|nil query execution time in milliseconds
---@return string[] lines
function M.format_results(results, elapsed_ms)
  local lines = {}

  for i, result in ipairs(results) do
    if i > 1 then
      lines[#lines + 1] = ""
    end

    if result.status == 2 or result.status == 9 then
      -- PGRES_TUPLES_OK or PGRES_SINGLE_TUPLE
      local tbl_lines = format_tuples(result)
      for _, l in ipairs(tbl_lines) do
        lines[#lines + 1] = l
      end
    elseif result.status == 1 then
      -- PGRES_COMMAND_OK
      lines[#lines + 1] = result.cmd_status or "OK"
    elseif result.error then
      local err = result.error:gsub("%s+$", "")
      -- Avoid "ERROR: ERROR:" duplication if PG message already starts with ERROR:
      if err:match("^ERROR:") then
        lines[#lines + 1] = err
      else
        lines[#lines + 1] = "ERROR: " .. err
      end
    else
      lines[#lines + 1] = "Unknown result status: " .. tostring(result.status)
    end
  end

  if elapsed_ms then
    lines[#lines + 1] = ""
    lines[#lines + 1] = string.format("Time: %.3f ms", elapsed_ms)
  end

  return lines
end

--- Get or create the result buffer for a source buffer.
---@param source_bufnr number
---@return number result_bufnr
local function get_result_buf(source_bufnr)
  local bufnr = result_buffers[source_bufnr]
  if bufnr and vim.api.nvim_buf_is_valid(bufnr) then
    return bufnr
  end

  bufnr = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_set_option_value("buftype", "nofile", { buf = bufnr })
  vim.api.nvim_set_option_value("bufhidden", "wipe", { buf = bufnr })
  vim.api.nvim_set_option_value("swapfile", false, { buf = bufnr })
  vim.api.nvim_set_option_value("filetype", "dbresult", { buf = bufnr })

  -- Give it a name based on the source buffer
  local source_name = vim.api.nvim_buf_get_name(source_bufnr)
  local short = vim.fn.fnamemodify(source_name, ":t:r")
  if short == "" then
    short = "untitled"
  end
  pcall(vim.api.nvim_buf_set_name, bufnr, "[simpledb: " .. short .. "]")

  result_buffers[source_bufnr] = bufnr

  -- Clean up the mapping when the result buffer is wiped
  vim.api.nvim_create_autocmd("BufWipeout", {
    buffer = bufnr,
    once = true,
    callback = function()
      result_buffers[source_bufnr] = nil
    end,
  })

  return bufnr
end

--- Find the window displaying a buffer, or nil.
---@param bufnr number
---@return number|nil winid
local function find_win_for_buf(bufnr)
  for _, win in ipairs(vim.api.nvim_list_wins()) do
    if vim.api.nvim_win_get_buf(win) == bufnr then
      return win
    end
  end
  return nil
end

--- Display result lines in the result split for a source buffer.
---@param source_bufnr number
---@param lines string[]
function M.show(source_bufnr, lines)
  local result_bufnr = get_result_buf(source_bufnr)
  local source_win = vim.api.nvim_get_current_win()

  -- Flatten any lines containing embedded newlines
  local flat = {}
  for _, line in ipairs(lines) do
    for sub in (line .. "\n"):gmatch("([^\n]*)\n") do
      flat[#flat + 1] = sub
    end
  end

  -- Write content
  vim.api.nvim_set_option_value("modifiable", true, { buf = result_bufnr })
  vim.api.nvim_buf_set_lines(result_bufnr, 0, -1, false, flat)
  vim.api.nvim_set_option_value("modifiable", false, { buf = result_bufnr })

  -- Find or create the split window
  local win = find_win_for_buf(result_bufnr)
  if not win then
    -- Open a bottom split
    vim.cmd("botright split")
    win = vim.api.nvim_get_current_win()
    vim.api.nvim_win_set_buf(win, result_bufnr)

    -- Reasonable height: half the window or the number of lines, whichever is smaller
    local height = math.min(#flat + 1, math.floor(vim.o.lines / 3))
    height = math.max(height, 5)
    vim.api.nvim_win_set_height(win, height)
  end

  -- Scroll result buffer to top
  vim.api.nvim_win_set_cursor(win, { 1, 0 })

  -- Return focus to the source window
  vim.api.nvim_set_current_win(source_win)
end

--- Display an error message in the result split.
---@param source_bufnr number
---@param err string
function M.show_error(source_bufnr, err)
  err = err:gsub("%s+$", "")
  if err:match("^ERROR:") then
    M.show(source_bufnr, { err })
  else
    M.show(source_bufnr, { "ERROR: " .. err })
  end
end

return M
