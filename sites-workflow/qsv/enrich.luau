-- Row-level enrichment for a CKAN sites crawl.
-- Adds: week, host, branch, status, vkey, ext, num_extensions, reachable
BEGIN {
-- Newest patch on each still-supported branch. Bump when CKAN ships a release,
-- otherwise every instance silently drops out of "current".
LATEST = {["2.11"] = "2.11.5", ["2.10"] = "2.10.10"}

MAX_BRANCH = 0
for br in pairs(LATEST) do
    local maj, min = br:match("^(%d+)%.(%d+)$")
    local n = tonumber(maj) * 1000 + tonumber(min)
    if n > MAX_BRANCH then MAX_BRANCH = n end
end

-- The extensions cell is usually a JSON list literal, but some portals report a
-- plain space-separated string. Splitting on every delimiter handles both, and
-- dedupes: one instance listing a plugin twice must not count twice.
function parse_exts(cell)
    if cell == nil or cell == "" then return nil end
    local seen, names = {}, {}
    for token in cell:gmatch("[^%[%]\"',%s]+") do
        if not seen[token] then
            seen[token] = true
            names[#names + 1] = token
        end
    end
    if #names == 0 then return nil end
    table.sort(names)
    return names
end
}!

local ext = parse_exts(extensions)
local flat = ext and table.concat(ext, "|") or ""
local n_ext = ext and #ext or 0

local version = ckan_version
local br = version ~= "" and version:match("^(%d+%.%d+)") or nil

local status
if version == "" then
    -- the portal told us nothing
    status = "unknown"
elseif br == nil then
    -- it answered with something unparseable (a bare "2", a fork string)
    status = "eol"
elseif LATEST[br] then
    status = version == LATEST[br] and "current" or "behind"
else
    local maj, min = br:match("^(%d+)%.(%d+)$")
    status = (tonumber(maj) * 1000 + tonumber(min)) > MAX_BRANCH and "current" or "eol"
end

-- Zero-padded so string comparison orders versions correctly downstream in SQL.
local vkey = ""
if version ~= "" then
    local parts = {}
    for d in version:gmatch("%d+") do
        parts[#parts + 1] = tonumber(d)
        if #parts == 3 then break end
    end
    while #parts < 3 do parts[#parts + 1] = 0 end
    vkey = string.format("%04d.%04d.%04d", parts[1], parts[2], parts[3])
end

local h = url:match("^%a[%w+.%-]*://([^/]+)") or url:match("^([^/]+)") or url
h = h:gsub("^www%.", "")

return {tstamp:sub(1, 10), h, br or "", status, vkey, flat, n_ext,
        (version ~= "" or n_ext > 0) and 1 or 0}
