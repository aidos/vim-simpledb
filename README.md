# vim-simpledb

Neovim plugin for executing PostgreSQL queries with persistent database connections.

Uses LuaJIT FFI to call libpq directly, keeping connections alive in the Neovim
process. No subprocess overhead per query -- the connection is established once
per buffer and reused. Queries execute asynchronously so Neovim never blocks.

Originally forked from [ivalkeen/vim-simpledb](https://github.com/ivalkeen/vim-simpledb),
now rewritten in Lua.

## Requirements

- **Neovim** 0.8+ (uses `vim.uv`, LuaJIT FFI, Lua APIs)
- **libpq** shared library (`libpq.so` / `libpq5`)
  - Already installed if you have `psql` on your system
  - Or: `apt install libpq5` / `brew install libpq`

No luarocks, no compiled modules, no other dependencies.

## Installation

With [lazy.nvim](https://github.com/folke/lazy.nvim):

```lua
{ "aidos/vim-simpledb" }
```

With [packer.nvim](https://github.com/wbthomason/packer.nvim):

```lua
use "aidos/vim-simpledb"
```

Or clone into your Neovim packages directory:

```sh
git clone https://github.com/aidos/vim-simpledb \
  ~/.local/share/nvim/site/pack/plugins/start/vim-simpledb
```

## Usage

1. Create or open a `.sql` file.

2. Put the connection string on line 1 as an SQL comment, using
   [libpq connection string format](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING):

   Key-value format:

       -- host=localhost user=postgres dbname=my_database

   URI format:

       -- postgresql://postgres@localhost/my_database

3. Optionally, add a **query wrapper** on lines 2+ (as comments, up to the
   first blank line). Use `{query}` as a placeholder:

       -- host=localhost dbname=mydb
       -- BEGIN; {query} ; ROLLBACK;

   Every executed query will be wrapped in a transaction that rolls back.

4. Write your SQL queries separated by blank lines.

5. **`<leader><Enter>`** in normal mode executes the current paragraph
   (text block between blank lines).

6. **`<Enter>`** in visual mode executes the selected lines.

The first time you execute a query in a buffer, simpledb connects to the
database (asynchronously). The connection persists for the lifetime of the
buffer -- subsequent queries reuse it with zero connection overhead.

Results appear in a read-only split below the SQL buffer.

## Commands

| Command | Description |
|---|---|
| `:SimpleDBExecuteSql` | Execute SQL (accepts a range, defaults to entire file) |
| `:SimpleDBDisconnect` | Close the current buffer's database connection |
| `:SimpleDBReconnect` | Reconnect (close + re-establish on next query) |
| `:SimpleDBStatus` | Show connection status for the current buffer |

## Configuration

```lua
-- Disable query timing display (default: 1 = enabled)
vim.g.simpledb_show_timing = 0

-- Disable default keybindings (default: 1 = enabled)
vim.g.simpledb_use_default_keybindings = 0
```

Set these in your `init.lua` before the plugin loads.

## How it works

- **LuaJIT FFI** calls libpq functions directly -- no shell subprocesses.
- **Async I/O**: connections and queries use libpq's non-blocking API,
  polled via `vim.uv` (libuv). Neovim stays responsive during queries.
- **Per-buffer connections**: each SQL buffer gets its own `PGconn*` that
  persists until the buffer is closed or you run `:SimpleDBDisconnect`.
- **Automatic cleanup**: connections are closed on `BufDelete`, `BufWipeout`,
  and `VimLeavePre`.
