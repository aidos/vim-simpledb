--- Expansion of psql-style meta-commands into SQL queries.
--- Supports a subset of common \d commands so users don't need to
--- remember the information_schema / pg_catalog queries.

local M = {}

--- Registry of meta-command patterns.
--- Each entry: { pattern, handler(captures...) -> sql }
--- Patterns are tested against the trimmed query text.
--- Order matters: more specific patterns must come first.
local commands = {}

--- Helper: quote a literal for use in a SQL string (single-quote escape).
---@param s string
---@return string
local function sql_literal(s)
  return "'" .. s:gsub("'", "''") .. "'"
end

--- Helper: split a possibly schema-qualified name into (schema, name).
--- Returns ("public", name) when no schema is given.
---@param identifier string  e.g. "my_schema.my_table" or "my_table"
---@return string schema
---@return string name
local function split_schema_table(identifier)
  local schema, name = identifier:match("^([^%.]+)%.(.+)$")
  if not schema then
    return "public", identifier
  end
  return schema, name
end

-- ── \dt  List tables ──────────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\dt%+%s+(.+)$",
  handler = function(name)
    local schema, tbl = split_schema_table(name)
    return string.format([[
SELECT
  c.relname                              AS "Name",
  pg_size_pretty(pg_total_relation_size(c.oid)) AS "Total Size",
  obj_description(c.oid, 'pg_class')    AS "Description"
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = %s
  AND c.relname = %s;]], sql_literal(schema), sql_literal(tbl))
  end,
}

commands[#commands + 1] = {
  pattern = "^\\dt%s+(.+)$",
  handler = function(name)
    local schema, tbl = split_schema_table(name)
    return string.format([[
SELECT schemaname AS "Schema", tablename AS "Name"
FROM pg_tables
WHERE schemaname = %s AND tablename = %s;]], sql_literal(schema), sql_literal(tbl))
  end,
}

commands[#commands + 1] = {
  pattern = "^\\dt%+%s*$",
  handler = function()
    return [[
SELECT
  n.nspname                              AS "Schema",
  c.relname                              AS "Name",
  pg_size_pretty(pg_total_relation_size(c.oid)) AS "Total Size",
  obj_description(c.oid, 'pg_class')    AS "Description"
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;]]
  end,
}

commands[#commands + 1] = {
  pattern = "^\\dt%s*$",
  handler = function()
    return [[
SELECT schemaname AS "Schema", tablename AS "Name"
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename;]]
  end,
}

-- ── \d table  Describe table (full overview) ──────────────────────────
--- Build the multi-statement SQL for \d table_name.
--- Each statement produces a separate result set; the display layer
--- renders them with a blank line between each section.
---@param schema string
---@param tbl string
---@param verbose boolean  true for \d+ (adds sizes, descriptions)
---@return string sql
local function describe_table(schema, tbl, verbose)
  local s, t = sql_literal(schema), sql_literal(tbl)
  local stmts = {}

  -- 1. Table identity
  stmts[#stmts + 1] = string.format([[
SELECT
  n.nspname                              AS "Schema",
  c.relname                              AS "Table",
  CASE c.relkind
    WHEN 'r' THEN 'table'
    WHEN 'p' THEN 'partitioned table'
    WHEN 'f' THEN 'foreign table'
  END                                    AS "Type"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s]], s, t)

  -- 2. Columns
  local col_extra = ""
  if verbose then
    col_extra = ",\n  pg_size_pretty(pg_catalog.pg_column_size(NULL::text)) AS \"Storage\","
      .. "\n  col_description(c.oid, a.attnum)                   AS \"Description\""
  end
  stmts[#stmts + 1] = string.format([[
SELECT
  a.attname                                          AS "Column",
  pg_catalog.format_type(a.atttypid, a.atttypmod)   AS "Type",
  CASE WHEN a.attnotnull THEN 'not null' ELSE '' END AS "Nullable",
  pg_get_expr(d.adbin, d.adrelid)                    AS "Default"%s
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = %s
  AND c.relname = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum]], col_extra, s, t)

  -- 3. Indexes
  stmts[#stmts + 1] = string.format([[
SELECT
  i.relname                              AS "Index",
  CASE WHEN ix.indisprimary THEN 'PRIMARY KEY'
       WHEN ix.indisunique  THEN 'UNIQUE'
       ELSE 'INDEX'
  END                                    AS "Type",
  pg_get_indexdef(ix.indexrelid)          AS "Definition"
FROM pg_catalog.pg_index ix
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
ORDER BY ix.indisprimary DESC, ix.indisunique DESC, i.relname]], s, t)

  -- 4. Constraints (check, unique, exclusion — PKs shown via indexes above)
  stmts[#stmts + 1] = string.format([[
SELECT
  con.conname                            AS "Constraint",
  CASE con.contype
    WHEN 'c' THEN 'CHECK'
    WHEN 'u' THEN 'UNIQUE'
    WHEN 'x' THEN 'EXCLUSION'
    WHEN 'p' THEN 'PRIMARY KEY'
  END                                    AS "Type",
  pg_get_constraintdef(con.oid)          AS "Definition"
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
  AND con.contype IN ('c', 'u', 'x', 'p')
ORDER BY con.contype, con.conname]], s, t)

  -- 5. Foreign keys (outgoing)
  stmts[#stmts + 1] = string.format([[
SELECT
  con.conname                            AS "FK Constraint",
  pg_get_constraintdef(con.oid)          AS "Definition"
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
  AND con.contype = 'f'
ORDER BY con.conname]], s, t)

  -- 6. Referenced by (incoming foreign keys from other tables)
  stmts[#stmts + 1] = string.format([[
SELECT
  con.conname                            AS "Referenced By",
  rn.nspname || '.' || rc.relname        AS "From Table",
  pg_get_constraintdef(con.oid)          AS "Definition"
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class rc ON rc.oid = con.conrelid
JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
JOIN pg_catalog.pg_class c ON c.oid = con.confrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
  AND con.contype = 'f'
ORDER BY rn.nspname, rc.relname, con.conname]], s, t)

  -- 7. Triggers
  stmts[#stmts + 1] = string.format([[
SELECT
  t.tgname                               AS "Trigger",
  pg_get_triggerdef(t.oid)               AS "Definition"
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s
  AND NOT t.tgisinternal
ORDER BY t.tgname]], s, t)

  -- 8. Size (verbose only)
  if verbose then
    stmts[#stmts + 1] = string.format([[
SELECT
  pg_size_pretty(pg_total_relation_size(c.oid))  AS "Total Size",
  pg_size_pretty(pg_relation_size(c.oid))        AS "Table Size",
  pg_size_pretty(pg_indexes_size(c.oid))         AS "Index Size",
  obj_description(c.oid, 'pg_class')             AS "Description"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s]], s, t)
  end

  return table.concat(stmts, ";\n") .. ";"
end

commands[#commands + 1] = {
  pattern = "^\\d%+%s+(.+)$",
  handler = function(name)
    local schema, tbl = split_schema_table(name)
    return describe_table(schema, tbl, true)
  end,
}

commands[#commands + 1] = {
  pattern = "^\\d%s+(.+)$",
  handler = function(name)
    local schema, tbl = split_schema_table(name)
    return describe_table(schema, tbl, false)
  end,
}

-- ── \d  List all relations ────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\d%s*$",
  handler = function()
    return [[
SELECT
  n.nspname                     AS "Schema",
  c.relname                     AS "Name",
  CASE c.relkind
    WHEN 'r' THEN 'table'
    WHEN 'v' THEN 'view'
    WHEN 'm' THEN 'materialized view'
    WHEN 'i' THEN 'index'
    WHEN 'S' THEN 'sequence'
    WHEN 'f' THEN 'foreign table'
    WHEN 'p' THEN 'partitioned table'
  END                           AS "Type"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','m','S','f','p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;]]
  end,
}

-- ── \di  List indexes ─────────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\di%s+(.+)$",
  handler = function(name)
    local schema, tbl = split_schema_table(name)
    return string.format([[
SELECT
  indexname  AS "Index",
  indexdef   AS "Definition"
FROM pg_indexes
WHERE schemaname = %s AND tablename = %s
ORDER BY indexname;]], sql_literal(schema), sql_literal(tbl))
  end,
}

commands[#commands + 1] = {
  pattern = "^\\di%s*$",
  handler = function()
    return [[
SELECT
  schemaname AS "Schema",
  tablename  AS "Table",
  indexname   AS "Index",
  indexdef    AS "Definition"
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename, indexname;]]
  end,
}

-- ── \dv  List views ───────────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\dv%s*$",
  handler = function()
    return [[
SELECT
  n.nspname AS "Schema",
  c.relname AS "Name"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;]]
  end,
}

-- ── \ds  List sequences ───────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\ds%s*$",
  handler = function()
    return [[
SELECT
  n.nspname AS "Schema",
  c.relname AS "Name"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'S'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, c.relname;]]
  end,
}

-- ── \dn  List schemas ─────────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\dn%s*$",
  handler = function()
    return [[
SELECT
  n.nspname                        AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY n.nspname;]]
  end,
}

-- ── \du / \dg  List roles ─────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\d[ug]%s*$",
  handler = function()
    return [[
SELECT
  r.rolname                     AS "Role",
  r.rolsuper                    AS "Superuser",
  r.rolcreaterole               AS "Create Role",
  r.rolcreatedb                 AS "Create DB",
  r.rolcanlogin                 AS "Login"
FROM pg_catalog.pg_roles r
ORDER BY r.rolname;]]
  end,
}

-- ── \df  List functions ───────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\df%s*$",
  handler = function()
    return [[
SELECT
  n.nspname                                    AS "Schema",
  p.proname                                    AS "Name",
  pg_catalog.pg_get_function_result(p.oid)     AS "Result",
  pg_catalog.pg_get_function_arguments(p.oid)  AS "Arguments"
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname, p.proname;]]
  end,
}

-- ── \l  List databases ────────────────────────────────────────────────
commands[#commands + 1] = {
  pattern = "^\\l%s*$",
  handler = function()
    return [[
SELECT
  d.datname                                   AS "Name",
  pg_catalog.pg_get_userbyid(d.datdba)        AS "Owner",
  pg_catalog.pg_encoding_to_char(d.encoding)  AS "Encoding"
FROM pg_catalog.pg_database d
ORDER BY d.datname;]]
  end,
}

--- Try to expand a query string as a meta-command.
--- Returns the expanded SQL if the input is a known meta-command,
--- or nil if it's not a meta-command (i.e. regular SQL).
---@param query string  trimmed query text
---@return string|nil expanded SQL, or nil
function M.expand(query)
  -- Quick check: meta-commands always start with a backslash
  if query:sub(1, 1) ~= "\\" then
    return nil
  end

  -- Trim trailing semicolons and whitespace to be forgiving
  local trimmed = query:match("^(.-)%s*;?%s*$")

  for _, cmd in ipairs(commands) do
    local captures = { trimmed:match(cmd.pattern) }
    if #captures > 0 or trimmed:match(cmd.pattern) then
      -- Trim whitespace from captures
      for i, cap in ipairs(captures) do
        captures[i] = cap:match("^%s*(.-)%s*$")
      end
      return cmd.handler(unpack(captures))
    end
  end

  return nil
end

return M
