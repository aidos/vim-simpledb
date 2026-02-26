--- Query extraction and wrapper template logic.

local M = {}

--- Extract the SQL query from a range of buffer lines.
--- Strips trailing -- comments from each line.
---@param bufnr number
---@param firstline number 1-indexed first line
---@param lastline number 1-indexed last line
---@return string query
function M.get_query(bufnr, firstline, lastline)
  local lines = vim.api.nvim_buf_get_lines(bufnr, firstline - 1, lastline, false)
  local fragments = {}
  for _, line in ipairs(lines) do
    -- Strip trailing SQL comments (-- ...) but preserve -- inside quoted strings.
    -- Simple approach: strip from the first -- that's not inside quotes.
    -- For robustness, we just strip trailing comments that have whitespace before --.
    local stripped = line:match("^(.-)%s+%-%-[^'\"]*$") or line
    stripped = stripped:match("^(.-)%s*$") -- trim trailing whitespace
    if stripped ~= "" then
      fragments[#fragments + 1] = stripped
    end
  end
  return table.concat(fragments, "\n")
end

--- Extract the query wrapper template from lines 2+ of the buffer.
--- Reads from line 2 until the first blank line.
--- Strips the leading "-- " comment prefix from each line.
--- If the result contains {query}, it is treated as a template.
---@param bufnr number
---@return string|nil wrapper template string, or nil if no wrapper found
function M.get_wrapper(bufnr)
  local line_count = vim.api.nvim_buf_line_count(bufnr)
  if line_count < 2 then
    return nil
  end

  local max_line = math.min(line_count, 100)
  local lines = vim.api.nvim_buf_get_lines(bufnr, 1, max_line, false)
  local fragments = {}

  for _, line in ipairs(lines) do
    -- Check if line is a comment
    local content = line:match("^%s*%-%-%s?(.*)")
    if not content then
      -- Not a comment line: stop at first non-comment (including blank lines)
      break
    end
    if content:match("^%s*$") then
      -- Empty comment line, treat as end of wrapper
      break
    end
    fragments[#fragments + 1] = content
  end

  if #fragments == 0 then
    return nil
  end

  local wrapper = table.concat(fragments, "\n")

  -- Only return if it contains the {query} placeholder
  if not wrapper:find("{query}", 1, true) then
    return nil
  end

  return wrapper
end

--- Apply the wrapper template to a query, if one exists.
---@param bufnr number
---@param query string
---@return string wrapped query
function M.apply_wrapper(bufnr, query)
  local wrapper = M.get_wrapper(bufnr)
  if wrapper then
    return (wrapper:gsub("{query}", query))
  end
  return query
end

return M
