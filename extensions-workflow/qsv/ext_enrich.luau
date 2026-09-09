-- Row-level enrichment for a CKAN extensions crawl.
-- Adds: week, owner, repo, days_since_release, maintenance, has_releases, contributor_band
--
-- The sites pipeline has qsv/enrich.luau; this is its counterpart, and the two
-- are deliberately shaped the same way so derive_ext_csvs.sh can mirror
-- derive_csvs.sh step for step.
BEGIN {
-- 2refresh.py writes the literal string "No releases" rather than an empty
-- cell when a repo has never tagged one. 1158 of 1455 extensions are in that
-- state, so this is the common path, not an edge case.
NO_RELEASE = "No releases"

-- Maintenance is measured against the crawl's own week, never against the
-- clock. A row archived a year ago must still say what it said then, otherwise
-- every historical bucket silently drifts on every rebuild.
MAINTENANCE = {
    {180,  "active"},      -- shipped within 6 months
    {365,  "maintained"},  -- within a year
    {730,  "dormant"},     -- within two
}

-- Days since the civil epoch (Howard Hinnant's algorithm). Luau has no date
-- library, and subtracting two of these is all the arithmetic needed.
function days_from_civil(y, m, d)
    if m <= 2 then y = y - 1 end
    local era = math.floor(y / 400)
    local yoe = y - era * 400
    local mp = (m + (m > 2 and -3 or 9))
    local doy = math.floor((153 * mp + 2) / 5) + d - 1
    local doe = yoe * 365 + math.floor(yoe / 4) - math.floor(yoe / 100) + doy
    return era * 146097 + doe - 719468
end

function to_days(iso)
    local y, m, d = iso:match("^(%d%d%d%d)-(%d%d)-(%d%d)")
    if y == nil then return nil end
    return days_from_civil(tonumber(y), tonumber(m), tonumber(d))
end

function band(n)
    if n == nil then return "" end
    if n <= 0 then return "none" end
    if n == 1 then return "solo" end
    if n <= 5 then return "small" end
    if n <= 20 then return "team" end
    return "large"
end
}!

local week = tstamp:sub(1, 10)

-- repository_name is "owner/repo". Anything else (a bare name, a blank) leaves
-- owner empty rather than guessing.
local owner, repo = repository_name:match("^([^/]+)/(.+)$")
if owner == nil then
    owner, repo = "", repository_name
end

local releases = tonumber(total_releases) or 0
local has_releases = releases > 0

local days, maintenance = "", "no_release"
if has_releases and release_date ~= NO_RELEASE then
    local rel = to_days(release_date)
    local now = to_days(week)
    if rel ~= nil and now ~= nil then
        local age = now - rel
        days = age
        -- A release dated after the crawl is a clock or API artifact, not a
        -- prediction; clamp it to "active" rather than emitting a negative age.
        if age < 0 then days = 0 end
        maintenance = "abandoned"
        for _, rule in ipairs(MAINTENANCE) do
            if days < rule[1] then
                maintenance = rule[2]
                break
            end
        end
    else
        -- It claims releases but the date will not parse: unknown, not stale.
        maintenance = "unknown"
    end
end

return {week, owner, repo, days, maintenance,
        has_releases and "true" or "false", band(tonumber(contributors_count))}
