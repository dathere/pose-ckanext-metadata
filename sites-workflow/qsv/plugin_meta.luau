-- Classifies a plugin name: is it shipped with CKAN, and which repo provides it.
BEGIN {
-- Plugins that ship inside CKAN itself. Everything else is third-party. Kept
-- explicit rather than inferred; the list moves between releases and guessing
-- produces worse answers than a stale list does.
CORE = {}
for _, p in ipairs({
    "activity", "datastore", "datapusher", "image_view", "text_view",
    "recline_view", "recline_grid_view", "recline_graph_view",
    "recline_map_view", "datatables_view", "webpage_view", "audio_view",
    "video_view", "resource_proxy", "stats", "multilingual",
    "expire_api_token", "tracking",
}) do CORE[p] = true end

-- Maps an individual plugin back to the repo that provides it, so
-- dcat + dcat_json_interface + dcat_rdf_harvester roll up to one package.
FAMILY = {
    {"^dcat", "ckanext-dcat"},
    {"_harvester$", "ckanext-harvest"},
    {"^harvest$", "ckanext-harvest"},
    {"^ckan_harvester$", "ckanext-harvest"},
    {"^spatial_", "ckanext-spatial"},
    {"^csw_", "ckanext-spatial"},
    {"^spatial$", "ckanext-spatial"},
    {"^geo_view$", "ckanext-geoview"},
    {"^geojson_view$", "ckanext-geoview"},
    {"^wmts_view$", "ckanext-geoview"},
    {"^shp_view$", "ckanext-geoview"},
    {"^ags_", "ckanext-geoview"},
    {"^scheming_", "ckanext-scheming"},
    {"^hierarchy_", "ckanext-hierarchy"},
    {"^pdf_view$", "ckanext-pdfview"},
    {"^officedocs_view$", "ckanext-officedocs"},
    {"^xloader$", "ckanext-xloader"},
    {"^pages$", "ckanext-pages"},
    {"^showcase$", "ckanext-showcase"},
    {"^envvars$", "ckanext-envvars"},
    {"^googleanalytics$", "ckanext-googleanalytics"},
}
}!

local plugin = name
local fam = ""
for _, rule in ipairs(FAMILY) do
    if plugin:match(rule[1]) then
        fam = rule[2]
        break
    end
end

return {CORE[plugin] and "true" or "false", fam}
