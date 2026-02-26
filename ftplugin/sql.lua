--- simpledb: filetype keybindings for SQL files.

if vim.g.simpledb_use_default_keybindings == 0 then
  return
end

-- Visual mode: select lines and press Enter to execute
vim.keymap.set("v", "<CR>", ":SimpleDBExecuteSql<CR>", {
  buffer = true,
  silent = true,
  desc = "Execute selected SQL via simpledb",
})

-- Normal mode: <leader>Enter executes the current paragraph
vim.keymap.set("n", "<leader><CR>", function()
  -- Save cursor position
  local pos = vim.api.nvim_win_get_cursor(0)

  -- Select paragraph range (text between blank lines)
  local bufnr = vim.api.nvim_get_current_buf()
  local current_line = pos[1]
  local line_count = vim.api.nvim_buf_line_count(bufnr)

  -- Find paragraph start (search backward for blank line or start of file)
  local first = current_line
  while first > 1 do
    local line = vim.api.nvim_buf_get_lines(bufnr, first - 2, first - 1, false)[1]
    if line:match("^%s*$") then
      break
    end
    first = first - 1
  end

  -- Find paragraph end (search forward for blank line or end of file)
  local last = current_line
  while last < line_count do
    local line = vim.api.nvim_buf_get_lines(bufnr, last, last + 1, false)[1]
    if not line or line:match("^%s*$") then
      break
    end
    last = last + 1
  end

  -- Execute the paragraph range
  require("simpledb").execute(first, last)

  -- Restore cursor position
  vim.api.nvim_win_set_cursor(0, pos)
end, {
  buffer = true,
  silent = true,
  desc = "Execute current SQL paragraph via simpledb",
})
