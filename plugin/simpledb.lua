--- simpledb: plugin entry point.
--- Defines user commands and global configuration defaults.

if vim.g.loaded_simpledb then
  return
end
vim.g.loaded_simpledb = true

-- Configuration defaults
if vim.g.simpledb_show_timing == nil then
  vim.g.simpledb_show_timing = 1
end
if vim.g.simpledb_use_default_keybindings == nil then
  vim.g.simpledb_use_default_keybindings = 1
end

-- User commands
vim.api.nvim_create_user_command("SimpleDBExecuteSql", function(opts)
  require("simpledb").execute(opts.line1, opts.line2)
end, {
  range = "%",
  desc = "Execute SQL query via persistent PostgreSQL connection",
})

vim.api.nvim_create_user_command("SimpleDBDisconnect", function()
  require("simpledb").disconnect()
end, {
  desc = "Disconnect the current buffer's PostgreSQL connection",
})

vim.api.nvim_create_user_command("SimpleDBStatus", function()
  require("simpledb").status()
end, {
  desc = "Show connection status for the current buffer",
})

vim.api.nvim_create_user_command("SimpleDBReconnect", function()
  require("simpledb").reconnect()
end, {
  desc = "Reconnect the current buffer's PostgreSQL connection",
})
