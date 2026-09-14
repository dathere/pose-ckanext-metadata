-- Row-level enrichment for a CKAN sites crawl.
-- Adds: week, host, branch, status, vkey, ext, num_extensions, reachable
BEGIN {
-- CKAN's release history, so "on latest patch" means the newest patch that
-- existed the week the crawl ran, not the newest patch that exists today.
-- Judging a 2025 crawl against a 2026 release made almost the whole archive
-- read as "behind on patches" and erased the week an instance actually was
-- current. Add a row to the CSV when CKAN ships; nothing here changes.
--
-- @CKAN_RELEASES@ is substituted with the CSV path by derive_csvs.sh, which is
-- the only supported way to run this script.
qsv_register_lookup("ckan_releases", "@CKAN_RELEASES@", 0)

-- branch -> patches, newest release first. Two patches of one branch never
-- ship on the same day, so the release date alone orders them.
RELEASES = {}
-- Branches CKAN still patches. The CSV carries the flag; anything false is end
-- of life whatever its patch level.
SUPPORTED = {}
for version, row in pairs(ckan_releases) do
    if type(row) == "table" and row.branch ~= nil then
        local br = row.branch
        RELEASES[br] = RELEASES[br] or {}
        table.insert(RELEASES[br], {version = version, released = row.released})
        SUPPORTED[br] = row.branch_supported == "true"
    end
end
for _, patches in pairs(RELEASES) do
    table.sort(patches, function(a, b) return a.released > b.released end)
end

MAX_BRANCH = 0
for br, supported in pairs(SUPPORTED) do
    if supported then
        local maj, min = br:match("^(%d+)%.(%d+)$")
        local n = tonumber(maj) * 1000 + tonumber(min)
        if n > MAX_BRANCH then MAX_BRANCH = n end
    end
end

-- Newest patch of `br` released on or before `day`, or nil when the branch had
-- not shipped yet — an instance can only be judged against releases that
-- existed when it was crawled.
function latest_as_of(br, day)
    local patches = RELEASES[br]
    if patches == nil then return nil end
    for _, patch in ipairs(patches) do
        if patch.released <= day then return patch.version end
    end
    return nil
end

-- Zero-padded so string comparison orders versions correctly, both downstream
-- in SQL and against the release table here.
function vkeyof(version)
    if version == "" then return "" end
    local parts = {}
    for d in version:gmatch("%d+") do
        parts[#parts + 1] = tonumber(d)
        if #parts == 3 then break end
    end
    while #parts < 3 do parts[#parts + 1] = 0 end
    return string.format("%04d.%04d.%04d", parts[1], parts[2], parts[3])
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
local day = tstamp:sub(1, 10)
local vkey = vkeyof(version)

local status
if version == "" then
    -- the portal told us nothing
    status = "unknown"
elseif br == nil then
    -- it answered with something unparseable (a bare "2", a fork string)
    status = "eol"
elseif SUPPORTED[br] then
    local latest = latest_as_of(br, day)
    if latest == nil or vkey >= vkeyof(latest) then
        -- nothing had shipped on this branch yet, or the instance is running
        -- something newer than the newest release: a dev build, not a laggard
        status = "current"
    else
        status = "behind"
    end
else
    local maj, min = br:match("^(%d+)%.(%d+)$")
    status = (tonumber(maj) * 1000 + tonumber(min)) > MAX_BRANCH and "current" or "eol"
end

local h = url:match("^%a[%w+.%-]*://([^/]+)") or url:match("^([^/]+)") or url
h = h:gsub("^www%.", "")

return {day, h, br or "", status, vkey, flat, n_ext,
        (version ~= "" or n_ext > 0) and 1 or 0}
